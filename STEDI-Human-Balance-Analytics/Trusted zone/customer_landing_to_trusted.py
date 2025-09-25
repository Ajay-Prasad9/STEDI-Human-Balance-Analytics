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

# Script generated for node Amazon S3
AmazonS3_node1757669153482 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-udacity-098/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1757669153482")

# Script generated for node PrivacyFilter
SqlQuery0 = '''
select * from myDataSource
where sharewithresearchasOfdate is not null
'''
PrivacyFilter_node1757669172402 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1757669153482}, transformation_ctx = "PrivacyFilter_node1757669172402")

# Script generated for node Trusted Customer Zone
EvaluateDataQuality().process_rows(frame=PrivacyFilter_node1757669172402, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1757668994078", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
TrustedCustomerZone_node1757669179640 = glueContext.getSink(path="s3://stedi-udacity-098/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrustedCustomerZone_node1757669179640")
TrustedCustomerZone_node1757669179640.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
TrustedCustomerZone_node1757669179640.setFormat("json")
TrustedCustomerZone_node1757669179640.writeFrame(PrivacyFilter_node1757669172402)
job.commit()