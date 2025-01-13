# proyecto-ingenieria-datos
Este proyecto ingesta información de un dataset público, transforma la informacion y crea una tabla de análisis con la data lista para ser consumida por equipos de análitica

Etapa de extracción (Extract)

El primer paso es crear los buckets S3 de entrada y salida. En uno de los buckets de entrada se cargará el archivo plano correspondiente a la información de accidentalidad de Estados Unidos entre febrero de 2016 a marzo de 2023. En el otro, se cargará un archivo csv correspondiente a los códigos y descripción de los estados en Estados Unidos. En cada uno de los buckets creados , se creó un AWS CRAWLER, con el fin de extraer la metadata de los archivos y cargarlos en AWS GLUE DATA CATALOG.  Los crawlers exploran los archivos de entrada (accidentalidad y parametrica) y almacenan la metadata en AWS GLUE DATA CATALOG. 

Etapa de transformación (Transform)

Teniendo la 2 tablas fuente disponibles en AWS DATACATALOG, se procede a realizar el proceso de transformación. Se creó un JOB en AWS GLUE.  A continuación se explica de forma detalla el código realizado:

* Importación de librerías
```
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

```

* Inicialización de sesión de spark empleando AWS GLUE

```
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


```

* Lectura y carga en dataframe de tabla fuente

```
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

```
 
* Consulta SQL - Spark
```
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

```

Como primer paso, se crea una tabla temporal (Common Table Expression) en donde se extrae de la columna start_time de la tabla datacloudcamp el año. Asimismo, se eliminaron los espacios en blanco de las columnas state y city. Luego, se hace una agrupación por los campos state, city y year_accidente (creada en el paso anterior), con el fin de hacer un conteo por año del campo id, es decir, hacer un conteo de los casos de accidentalidad por año, estado y ciudad.
Con base a la tabla temporal anteriormente creada, se añade un nuevo campo llamado row_num con el fin de crear un contador que enumera por año las ciudades con mayor número de accidentalidad.Finalmente, se añade un filtro para ver solo el top 5 de ciudades con mayor accidentalidad y se hace un cruce con la tabla paramétrica para convertir los códigos de los estados en el nombres de los estados en sí.



* Comando para ejecutar consulta SQL - Spark y cargar los resultados en el dataframe df_result.
```
df_result = spark.sql(query)

```


* Escritura en el bucket: data-cloudcamp-output, en formato parquet del dataframe que contiene el resultado de la query ejecutada.

```
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

```

Etapa de Carga (Load)

Para ese punto, ya se generó el archivo parquet con los resultados de la pregunta de investigación. El siguiente paso es cargar a AWS DATACATALOG en formato tabla el archivo parquet generado. Para ese fin se creó el crawler.
