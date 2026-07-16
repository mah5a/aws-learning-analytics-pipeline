import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
Rules = [
    ColumnCount = 9,
    RowCount > 0,
    IsComplete "id_student",
    IsUnique "id_student",
    ColumnValues "average_score" between 0 and 100,
    ColumnValues "total_clicks" >= 0,
    ColumnValues "avg_daily_clicks" >= 0,
    ColumnValues "assessment_count" > 0
]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1783449003727 = glueContext.create_dynamic_frame.from_catalog(database="oulad_db", table_name="studentassessment", transformation_ctx="AWSGlueDataCatalog_node1783449003727")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1783448398526 = glueContext.create_dynamic_frame.from_catalog(database="oulad_db", table_name="studentvle", transformation_ctx="AWSGlueDataCatalog_node1783448398526")

# Script generated for node Aggregate
Aggregate_node1783971840622 = sparkAggregate(glueContext, parentFrame = AWSGlueDataCatalog_node1783449003727, groups = ["id_student"], aggs = [["score", "avg"], ["score", "max"], ["score", "min"], ["id_assessment", "count"]], transformation_ctx = "Aggregate_node1783971840622")

# Script generated for node Aggregate
Aggregate_node1783451498928 = sparkAggregate(glueContext, parentFrame = AWSGlueDataCatalog_node1783448398526, groups = ["id_student"], aggs = [["sum_click", "sum"], ["sum_click", "avg"], ["date", "min"], ["date", "max"]], transformation_ctx = "Aggregate_node1783451498928")

# Script generated for node studentassessment_features
studentassessment_features_node1783972460403 = ApplyMapping.apply(frame=Aggregate_node1783971840622, mappings=[("id_student", "string", "id_student", "long"), ("`avg(score)`", "double", "average_score", "double"), ("`max(score)`", "string", "max_score", "string"), ("`min(score)`", "string", "min_score", "string"), ("`count(id_assessment)`", "long", "assessment_count", "long")], transformation_ctx="studentassessment_features_node1783972460403")

# Script generated for node studentvle_features
studentvle_features_node1783453998469 = ApplyMapping.apply(frame=Aggregate_node1783451498928, mappings=[("id_student", "long", "id_student", "long"), ("`sum(sum_click)`", "long", "total_clicks", "long"), ("`avg(sum_click)`", "double", "avg_daily_clicks", "double"), ("`min(date)`", "long", "first_activity_day", "long"), ("`max(date)`", "long", "last_activity_day", "long")], transformation_ctx="studentvle_features_node1783453998469")

# Script generated for node Join
Join_node1783974481776 = Join.apply(frame1=studentvle_features_node1783453998469, frame2=studentassessment_features_node1783972460403, keys1=["id_student"], keys2=["id_student"], transformation_ctx="Join_node1783974481776")

# Script generated for node Change Schema after join
ChangeSchemaafterjoin_node1783989704179 = ApplyMapping.apply(frame=Join_node1783974481776, mappings=[("id_student", "long", "id_student", "long"), ("total_clicks", "long", "total_clicks", "long"), ("avg_daily_clicks", "double", "avg_daily_clicks", "double"), ("first_activity_day", "long", "first_activity_day", "long"), ("last_activity_day", "long", "last_activity_day", "long"), ("average_score", "double", "average_score", "double"), ("max_score", "string", "max_score", "double"), ("min_score", "string", "min_score", "double"), ("assessment_count", "long", "assessment_count", "long")], transformation_ctx="ChangeSchemaafterjoin_node1783989704179")

# Script generated for node Amazon S3
# EvaluateDataQuality().process_rows(frame=studentassessment_features_node1783972460403, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783968849198", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
# AmazonS3_node1783973871305 = glueContext.write_dynamic_frame.from_options(frame=studentassessment_features_node1783972460403, connection_type="s3", format="glueparquet", connection_options={"path": "s3://oulad-data-engineering/curated/studentassessment_features/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1783973871305")

# Script generated for node Amazon S3
# EvaluateDataQuality().process_rows(frame=studentvle_features_node1783453998469, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783448358437", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
# AmazonS3_node1783452491182 = glueContext.write_dynamic_frame.from_options(frame=studentvle_features_node1783453998469, connection_type="s3", format="glueparquet", connection_options={"path": "s3://oulad-data-engineering/curated/studentvle_features/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1783452491182")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchemaafterjoin_node1783989704179, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783968849198", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1783975642035 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchemaafterjoin_node1783989704179, connection_type="s3", format="glueparquet", connection_options={"path": "s3://oulad-data-engineering/curated/student_features/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1783975642035")

job.commit()
