"""
Este código realiza el proceso de transformacion dentro de un proceso ETL. Lee las 2 tablas:
* data_cloudcamp (tabla que recopila casos de accidentalidad en EU)
* data_cloudcamp_parametrictable (tabla parametrica que relaciona codigo con despcripcion de los Estados en EU)
Luego, mediante un query limpia y transforma la data, con el fin de obtener el top 5 de los estados
con más accidentalidad en los Estados Unidos desde 2016 hasta 2023. Finalmente carga dicha información aun
bucket S3 de salida en formato parquet.

"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


dyf_accidents = glueContext.create_dynamic_frame.from_catalog(
    database="dbdatacloudcamp", 
    table_name="data_cloudcamp",
    transformation_ctx="dyf_accidents"
)

dyf_parametric = glueContext.create_dynamic_frame.from_catalog(
    database="dbdatacloudcamp",
    table_name="data_cloudcamp_parametrictable",
    transformation_ctx="dyf_parametric"
)

df_accidents = dyf_accidents.toDF()
df_parametric = dyf_parametric.toDF()

# Registrar los DataFrames como tablas temporales
df_accidents.createOrReplaceTempView("data_cloudcamp")
df_parametric.createOrReplaceTempView("data_cloudcamp_parametrictable")


query = """
WITH temporary_table AS (
    SELECT COUNT(id) AS numbers_accidents,
           TRIM(BOTH '"' FROM state) AS state,
           TRIM(BOTH '"' FROM city) AS city,
           YEAR(DATE(TRIM(BOTH '"' FROM SPLIT(start_time, ' ')[0]))) AS year_accident
    FROM data_cloudcamp
    WHERE TRIM(state) IS NOT NULL AND TRIM(city) IS NOT NULL
    GROUP BY state, city, year_accident
    ORDER BY year_accident DESC, numbers_accidents DESC
)
SELECT a.numbers_accidents,
       a.state AS code_state,
       a.city,
       a.year_accident,
       b.col0 AS name_state
FROM (
    SELECT numbers_accidents,
           state,
           city,
           year_accident,
           ROW_NUMBER() OVER (PARTITION BY year_accident ORDER BY numbers_accidents DESC) AS row_num
    FROM temporary_table
) AS a
LEFT JOIN data_cloudcamp_parametrictable AS b
ON a.state = b.col1
WHERE a.row_num <= 5
ORDER BY a.year_accident DESC, a.numbers_accidents DESC
"""

# Ejecutar el query
df_result = spark.sql(query)

df_transformed = df_result
dyf_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dyf_transformed")
glueContext.write_dynamic_frame.from_options(
    frame = dyf_transformed,
    connection_type = "s3",
    connection_options = {"path": "s3://data-cloudcamp-output"},
    format = "parquet",
    transformation_ctx = "write_parquet"
)
job.commit()