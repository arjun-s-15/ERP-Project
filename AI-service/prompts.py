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

1. Use only columns present in the dataset schema unless creating a new feature via feature engineering.

2. If a canonical feature can be mapped directly, use a rename operation.

3. If a datatype does not match the canonical requirement, add a cast operation.

4. If a timestamp column exists but is not datetime type, use to_datetime and set target_dtype.

5. If a canonical feature can be derived from multiple columns, use feature_engineering.

6. If a canonical feature is marked as required but does not exist in the dataset:
   - Create it using feature_engineering if possible
   - Otherwise, generate it using available columns (e.g., concatenation)
   - Set allow_if_missing = true if generation depends on optional columns

7. If a canonical feature is optional and cannot be mapped or derived:
   - Skip it (do not include unnecessary operations)

8. Do NOT force mappings for columns that do not semantically match the canonical feature.

9. The operations list should be ordered logically.

10. Operations must be executable sequentially. If a column is renamed, all later operations must reference the new column name.

11. Do NOT drop columns unless they are completely irrelevant or duplicate.

12. Additional columns not part of the canonical feature set should be preserved, as they may be useful for downstream tasks such as feature engineering, analytics, or modeling.

13. Only drop columns if they are clearly unnecessary or cannot be used meaningfully.


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

2. Only modify or add steps necessary to resolve the validation errors.

3. Ensure all required canonical features are produced:
   - If missing, create them using feature_engineering when possible.

4. If a required feature cannot be directly mapped:
   - Derive or generate it using available columns (e.g., concatenation).

5. If a canonical feature is optional and cannot be mapped or derived:
   - Skip it (do not introduce unnecessary operations).

6. Ensure datatypes match the expected canonical datatype.

7. Operations must be logically ordered and executable sequentially.

8. If a column is renamed, all later operations must reference the new column name.

9. Use only columns present in the dataset schema unless performing feature engineering.

10. When creating or deriving features, prefer simple deterministic formulas (e.g., concat, arithmetic).

11. Use allow_if_missing = true for feature_engineering steps that depend on columns that may not always exist.

12. Do NOT add data cleaning logic such as filtering negative values or removing rows.

13. Return a complete TransformationPlan.

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
- Make sure transformation_plan is not used as a data structure in the code.
- The correct source and target columns must be used.
- Column renames must match exactly.
- Type conversions must match the target dtype.
- Dropped columns must be removed.

Return your answer as JSON with this structure:
{code_validation_result}
"""

