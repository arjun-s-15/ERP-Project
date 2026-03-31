from enum import Enum
import pandas as pd
from pydantic import BaseModel, Field
from typing import TypedDict, Optional, List 

class OperationType(str, Enum):
    RENAME = "rename"
    CAST = "cast" 
    TO_DATETIME = "to_datetime" 
    FILLNA = "fillna" 
    DROP_COLUMNS = "drop_columns" 
    FEATURE_ENGINEERING = "feature_engineering"

class TransformationStep(BaseModel):
    type: OperationType = Field(description="Type of transformation operation")
    source_column: Optional[str] = Field(default=None, description="Original column name in the dataset")
    source_dtype: Optional[str] = Field(default=None, description="Original column datatype")
    target_column: Optional[str] = Field(default=None, description="Column name after transformation")
    target_dtype: Optional[str] = Field(default=None, description="Expected datatype after transformation")
    source_columns: Optional[List[str]] = Field(default=None, description="Multiple columns used for feature engineering")
    formula: Optional[str] = Field(default=None, description="Formula used for feature engineering")
    fill_value: Optional[str] = Field(default=None, description="Value used for missing value imputation")
    allow_if_missing: Optional[bool] = Field(default=False, description="Allowing column to be skipped if it's missing in the dataset schema.")

class TransformationPlan(BaseModel):
    operations: List[TransformationStep] = Field(description="Ordered list of transformations required to convert the dataset to canonical format")

class CodeValidationResult(BaseModel):
    valid: bool = Field(default=False, description="Final output whether is the code is valid.")
    missing_operations: List[str] = Field(default=[], description="List of operations that are missing in the code." )
    incorrect_operations: List[str] = Field(default=[], description="List of incorrect operations in the code." )
    reasoning: str = Field(description="The reasoning behind the validation output.")

class GraphState(TypedDict):
    input_path: str
    output_path: str
    schema: dict
    code: str 
    feedback: CodeValidationResult
    transformation_plan: TransformationPlan
    execution_error: str
    validation_passed: bool 
    validator_feedback: str