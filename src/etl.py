import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.job import job
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext

import libs.function as Func
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def extract_transformation_load():
    """
    =================================
    データ読み込み処理の全体をこちらに記載する
    =================================
    """ 
    # S3並びにDataCatalogのデータを読み込む


    # 参考) DataCatalog読み込み
    # dynamic_frame = glueContext.create_dynamic_frame \
    #                            .from_catalog( 
    #                                database="database", 
    #                                table_name= "table_name" 
    #                                )

    # S3
    dynamic_frame = glueContext.create_dynamic_frame \
                                .from_options( \
                                    connection_type="s3",
                                    connection_options={
                                        "paths": ["s3://{0}".format("bucket_name")]},
                                        format="csv", 
                                        format_options={"withHeader": True}
                                    )

    # SparkDataFrame上での処理に変換する
    data_frame = dynamic_frame.toDF()

    """
    =================================
    ETL処理をこちらに記載する
    =================================
    """ 
    # libs/functionの関数を呼び出す
    data_frame = Func.data_processing(dataframe)

    # DynamicFrameに再変換する
    dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext)

    # Mappingを割り当てる
    dynamic_frame = ApplyMapping.apply(frame = data_frame,
                                        mappings = Func.mapping_rule()
                                        )
    """
    =================================
    シンク処理をこちらに記載する
    =================================
    """     
    # datasink処理を記載する
    datasink = glueContext.write_dynamic_frame
                            .from_options(
                                frame=dynamic_frame,
                                conection_type="s3",
                                connection_options= {"path": "s3://PATH"},
                                format="parquet") # csv, jsonでもよい
try:
    extract_transformation_load()
except:
    print("Error: ETL processing was failed")
job.commit()