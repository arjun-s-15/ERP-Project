TRANSFORMATION_PLANNER_PROMPT = """
You are a senior data engineer responsible for standardizing datasets.

Your task is to analyze a dataset schema and generate a transformation plan
that converts the dataset into a canonical feature set.

The output MUST follow the Pydantic schema for TransformationPlan.

--------------------------------

DATASET SCHEMA

{dataset_schema}

--------------------------------

CANONICAL FEATURE SET

Each canonical feature represents a business concept required by the analytics system.

For each feature you are given:
- feature name
- description
- expected datatype

{canonical_features}

--------------------------------

TRANSFORMATION GOAL

Generate a TransformationPlan that converts the dataset into the canonical feature set.

The transformation may include:

• Renaming columns
• Casting datatypes
• Converting timestamps
• Filling missing values
• Dropping unused columns
• Feature engineering from multiple columns

--------------------------------

RULES

1. Use only columns present in the dataset schema.
2. Do NOT invent new columns unless performing feature engineering.
3. If a canonical feature can be mapped directly, use a rename operation.
4. If a datatype does not match the canonical requirement, add a cast operation.
5. If a timestamp column exists but is not datetime type, use to_datetime. And make sure to set target_dtype.
6. If a canonical feature must be derived from multiple columns, use feature_engineering.
7. The operations list should be ordered logically.
8. Operations must be executable sequentially. If a column is renamed, all later operations must reference the new column name.

--------------------------------

OUTPUT FORMAT

Return ONLY valid JSON that matches the following structure:

{transformation_plan}

Do NOT include explanations.
Do NOT include markdown.
Return ONLY valid JSON.
"""

TRANSFORMATION_REPLANNER_PROMPT = """
You are a senior data engineer responsible for correcting a transformation plan.
The previous transformation plan was executed but failed validation.
Your task is to adjust the plan so that the resulting dataset satisfies the canonical feature set.
The output MUST follow the Pydantic schema for TransformationPlan.

--------------------------------

CANONICAL FEATURE SET

{canonical_features}

--------------------------------

PREVIOUS TRANSFORMATION PLAN

{previous_plan}

--------------------------------

POST VALIDATION ERRORS

The following structural validation errors occurred after executing the plan:

{validator_errors}

--------------------------------

TASK

Modify the previous transformation plan so that the validation errors are resolved.

Only make changes that are necessary to fix the errors.

--------------------------------

RULES

1. Preserve correct operations from the previous plan whenever possible.
2. Only modify steps related to the validation errors.
3. Ensure all required canonical features are produced.
4. Ensure datatypes match the expected canonical datatype.
5. Operations must be logically ordered and executable sequentially.
6. If a column is renamed, all later operations must reference the new column name.
7. Use only columns present in the dataset schema unless performing feature engineering.
8. Do NOT add data cleaning logic such as filtering negative values or removing rows.
9. Return a complete TransformationPlan.

--------------------------------

OUTPUT FORMAT

Return ONLY valid JSON matching the TransformationPlan schema.

{transformation_plan}

Do NOT include explanations.
Do NOT include markdown.
Return ONLY valid JSON.
"""

CODE_GENERATOR_PROMPT = """
Generate Python pandas code that performs the following transformation plan.
Rules:
- Assume dataframe is named 'df' 
- Perform operations sequentially in the same order as the plan.
- Return ONLY Python code.

Transformation Plan:
{transformation_plan}
"""

CODE_GENERATOR_RETRY_PROMPT = """
The previous code failed.
Transformation Plan:
{transformation_plan}

Previous Code:
{code}

Feedback:
{feedback}

Fix the code so the transformation executes successfully.
Return ONLY Python code.
"""

CODE_VALIDATOR_PROMPT = """
You are a senior data engineer validating transformation code.

Your job is to check whether the generated pandas code correctly implements the provided transformation plan.

You are NOT executing the code.
You are only verifying that the intent of each operation is implemented.

Transformation Plan:
{plan}

Generated Code:
{code}

Validation Rules:
- Every operation in the plan must appear in the code.
- The correct source and target columns must be used.
- Column renames must match exactly.
- Type conversions must match the target dtype.
- Dropped columns must be removed.

Return your answer as JSON with this structure:
{code_validation_result}
"""

