from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_student_features(df: DataFrame) -> None:
    """
    Validate the final student_features dataset.

    Raises:
        ValueError: If a data quality rule fails.
    """

    required_columns = [
        "id_student",
        "code_module",
        "code_presentation",
        "average_score",
        "total_clicks",
    ]

    # Check required columns
    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    # Check empty dataset
    total_rows = df.count()

    if total_rows == 0:
        raise ValueError("Data quality failed: dataset is empty.")

    # Check null student IDs
    null_student_ids = df.filter(
        F.col("id_student").isNull()
    ).count()

    if null_student_ids > 0:
        raise ValueError(
            f"Data quality failed: {null_student_ids} null id_student values."
        )

    # Check duplicate student-module-presentation records
    duplicate_records = (
        df.groupBy(
            "id_student",
            "code_module",
            "code_presentation",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_records > 0:
        raise ValueError(
            f"Data quality failed: {duplicate_records} duplicate records."
        )

    # Assessment scores must be between 0 and 100
    invalid_scores = df.filter(
        (F.col("average_score") < 0)
        | (F.col("average_score") > 100)
    ).count()

    if invalid_scores > 0:
        raise ValueError(
            f"Data quality failed: {invalid_scores} invalid average_score values."
        )

    # Click counts cannot be negative
    negative_clicks = df.filter(
        F.col("total_clicks") < 0
    ).count()

    if negative_clicks > 0:
        raise ValueError(
            f"Data quality failed: {negative_clicks} negative total_clicks values."
        )

    print("Data quality validation passed.")
    print(f"Validated rows: {total_rows}")
