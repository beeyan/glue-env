import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "sampledb", table_name = "elb_logs", transformation_ctx = "datasource0")
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("request_timestamp", "string", "request_timestamp", "string"), ("elb_name", "string", "elb_name", "string"), ("request_ip", "string", "request_ip", "string"), ("request_port", "int", "request_port", "int"), ("backend_ip", "string", "backend_ip", "string"), ("backend_port", "int", "backend_port", "int"), ("request_processing_time", "double", "request_processing_time", "double"), ("backend_processing_time", "double", "backend_processing_time", "double"), ("client_response_time", "double", "client_response_time", "double"), ("elb_response_code", "string", "elb_response_code", "string"), ("backend_response_code", "string", "backend_response_code", "string"), ("received_bytes", "long", "received_bytes", "long"), ("sent_bytes", "long", "sent_bytes", "long"), ("request_verb", "string", "request_verb", "string"), ("url", "string", "url", "string"), ("protocol", "string", "protocol", "string"), ("user_agent", "string", "user_agent", "string"), ("ssl_cipher", "string", "ssl_cipher", "string"), ("ssl_protocol", "string", "ssl_protocol", "string")], transformation_ctx = "applymapping1")
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": "s3://sample-glue-for-results"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()