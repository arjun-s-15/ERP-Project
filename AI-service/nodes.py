from io import StringIO
import os
import traceback
from typing import Literal

from langchain.messages import HumanMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import END
import pandas as pd
import boto3
from dotenv import load_dotenv
load_dotenv()

from models import GraphState, TransformationPlan, CodeValidationResult
from prompts import CODE_GENERATOR_PROMPT, CODE_GENERATOR_RETRY_PROMPT, CODE_VALIDATOR_PROMPT, TRANSFORMATION_PLANNER_PROMPT, TRANSFORMATION_REPLANNER_PROMPT
from utils import clean_code, canonical_feature_set

GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

def get_schema_node(state: GraphState):
    file_path = state.get('file_path', None)
    schema = {}
    if file_path:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_path)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))

        schema = {
            "num_rows": df.shape[0],
            "num_columns": df.shape[1],
            "columns": {}
        }
        for col in df.columns:
            col_data = df[col]
            if pd.api.types.is_numeric_dtype(col_data):
                col_type = "numeric"
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                col_type = "datetime"
            elif pd.api.types.is_bool_dtype(col_data):
                col_type = "boolean"
            else:
                col_type = "categorical"
            col_info = {
                "dtype": str(col_data.dtype),
                "missing_values": int(col_data.isna().sum()),
                "missing_percentage": float(col_data.isna().mean()),
                "unique_values": int(col_data.nunique()),
                "sample_values": col_data.dropna().unique()[:3].tolist(),
                "semantic_type": col_type
            }
            schema["columns"][col] = col_info

    return {"schema": schema}

def transformation_planner_node(state: GraphState):
    schema = state.get('schema', {})
    validator_feedback = state.get('validator_feedback', None)
    previous_plan = state.get("transformation_plan", None)
    errors = validator_feedback.get("errors") if validator_feedback else None # type: ignore

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        api_key=GOOGLE_API_KEY
    )

    if errors and previous_plan:
        prompt_text = TRANSFORMATION_REPLANNER_PROMPT.format(
            canonical_features=canonical_feature_set,
            previous_plan=previous_plan,
            validation_errors=errors,
            transformation_plan=TransformationPlan.model_json_schema()
        )
    else:
        prompt_text = TRANSFORMATION_PLANNER_PROMPT.format(
            dataset_schema=schema,
            canonical_features=canonical_feature_set,
            transformation_plan=TransformationPlan.model_json_schema()
        )

    structured_llm = llm.with_structured_output(TransformationPlan, method='function_calling')
    response = structured_llm.invoke([HumanMessage(content=prompt_text)])
    return {"transformation_plan": response.model_dump()} # type: ignore

def code_generator_node(state: GraphState):
    plan = state.get('transformation_plan', [])
    feedback = state.get('feedback', None)
    existing_code = state.get('code', None)

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        api_key=GOOGLE_API_KEY
    )
    if existing_code and feedback:
        prompt_text = CODE_GENERATOR_RETRY_PROMPT.format(
            transformation_plan = plan,
            code=existing_code,
            feedback=feedback
        )
    else:
        prompt_text = CODE_GENERATOR_PROMPT.format(
            transformation_plan=plan
        )
    response = llm.invoke([HumanMessage(content=prompt_text)])

    return {"code": response.content}

def code_validator_node(state: GraphState):
    plan = state.get('transformation_plan', [])
    code = state.get('code', None)

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        api_key=GOOGLE_API_KEY
    )

    structured_llm = llm.with_structured_output(CodeValidationResult, method='function_calling')
    messages = [
        HumanMessage(content=CODE_VALIDATOR_PROMPT.format(
            plan=plan,
            code=code,
            code_validation_result=CodeValidationResult.model_json_schema()
        ))
    ]
    response = structured_llm.invoke(messages)
    return {"feedback": response.model_dump()} # type: ignore

def code_validation_routing(state: GraphState) -> Literal["code_generator_node", "executor_node"]:
    feedback = state.get('feedback', {})
    if feedback["valid"]: # type: ignore
        return "executor_node"
    else:
        return "code_generator_node"

def executor_node(state: GraphState):
    file_path = state.get('file_path', '')
    code = state.get('code', '')
    try:
        df = pd.read_csv(file_path)

        local_vars = {
            "df": df,
        }
        code = clean_code(code)
        exec(code, {}, local_vars)
        transformed_df = local_vars["df"]
        return {"transformed_df": transformed_df, "execution_error": None}
    except Exception:
        return {"execution_error": traceback.format_exc()}
    
def post_validator_node(state: GraphState):
    df = state["transformed_df"]
    errors = []
    warnings = []

    # Normalize column names for safety
    df.columns = df.columns.str.strip()

    for feature in canonical_feature_set:
        name = feature["name"]
        required = feature.get("required", False)
        expected_dtype = feature.get("expected_dtype")
        constraints = feature.get("constraints", {})

        # -----------------------------
        # 1. Required column check
        # -----------------------------
        if required and name not in df.columns:
            errors.append(f"Missing required column: {name}")
            continue

        # Skip validation if optional column not present
        if name not in df.columns:
            continue

        col = df[name]

        # -----------------------------
        # 2. Expected dtype validation
        # -----------------------------
        if expected_dtype == "string":
            if not pd.api.types.is_string_dtype(col) and not pd.api.types.is_object_dtype(col):
                errors.append(f"{name} should be string dtype")

        elif expected_dtype == "numeric":
            if not pd.api.types.is_numeric_dtype(col):
                errors.append(f"{name} should be numeric dtype")

        elif expected_dtype == "datetime":
            if not pd.api.types.is_datetime64_any_dtype(col):
                errors.append(f"{name} should be datetime dtype")

        # -----------------------------
        # 3. Constraint checks
        # -----------------------------
        if constraints.get("must_be_numeric"):
            if not pd.api.types.is_numeric_dtype(col):
                errors.append(f"{name} must be numeric")

        if constraints.get("must_be_positive"):
            if (col < 0).any():
                warnings.append(f"{name} contains negative values")

        if constraints.get("must_be_datetime_compatible"):
            if not pd.api.types.is_datetime64_any_dtype(col):
                errors.append(f"{name} must be datetime compatible")

    # -----------------------------
    # Final validation result
    # -----------------------------
    validation_passed = len(errors) == 0

    return {
        "validation_passed": validation_passed,
        "validator_feedback": {"errors": errors if errors else None, "warnings": warnings if warnings else None}
    }

def post_validation_routing(state: GraphState) -> Literal["transformation_planner_node", END]: # type: ignore
    final_validation = state.get("validation_passed", False)
    if final_validation:
        return END
    else:
        return "transformation_planner_node"