import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
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
        ColumnCount > 0
    ]
"""

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1758777906660 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-udacity-098/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1758777906660")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1758777904424 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-udacity-098/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1758777904424")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
    t.sensorreadingtime,
    t.serialnumber,
    t.distancefromobject,
    a.user,
    a.timestamp,
    a.x,
    a.y,
    a.z
from t
INNER JOIN a
ON t.sensorreadingtime = a.timestamp;
'''
SQLQuery_node1758778006229 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"t":StepTrainerTrusted_node1758777906660, "a":AccelerometerTrusted_node1758777904424}, transformation_ctx = "SQLQuery_node1758778006229")

# Script generated for node Machine Learning Curate
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758778006229, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758776926658", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurate_node1758778171702 = glueContext.getSink(path="s3://stedi-udacity-098/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurate_node1758778171702")
MachineLearningCurate_node1758778171702.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurate_node1758778171702.setFormat("json")
MachineLearningCurate_node1758778171702.writeFrame(SQLQuery_node1758778006229)
job.commit()