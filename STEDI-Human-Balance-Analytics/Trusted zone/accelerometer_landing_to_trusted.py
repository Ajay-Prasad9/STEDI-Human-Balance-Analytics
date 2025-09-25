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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1757921526642 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1757921526642")

# Script generated for node Customer Trusted
CustomerTrusted_node1757922449302 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1757922449302")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
    a.user,
    a.timestamp,
    a.x,
    a.y,
    a.z
FROM c
INNER JOIN a 
    on c.email = a.user;
'''
SQLQuery_node1758773618879 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"a":AccelerometerLanding_node1757921526642, "c":CustomerTrusted_node1757922449302}, transformation_ctx = "SQLQuery_node1758773618879")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758773618879, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757916481807", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1757921537763 = glueContext.getSink(path="s3://stedi-udacity-098/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1757921537763")
AccelerometerTrusted_node1757921537763.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1757921537763.setFormat("json")
AccelerometerTrusted_node1757921537763.writeFrame(SQLQuery_node1758773618879)
job.commit()