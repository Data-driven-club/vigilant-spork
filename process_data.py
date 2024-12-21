from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, sum as spark_sum

# Crear sesión Spark
spark = SparkSession.builder.appName("ProcessLargeData").getOrCreate()

# Leer archivo comprimido
data_path = "data.gz"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Crear las columnas combinadas necesarias
df = df.withColumn("TIPO_GOBIERNO", concat_ws("-", col("TIPO_GOBIERNO"), col("TIPO_GOBIERNO_NOM"))) \
       .withColumn("SECTOR", concat_ws("-", col("SECTOR"), col("SECTOR_NOM"))) \
       .withColumn("TIPO_ACT_PROY", concat_ws("-", col("TIPO_ACT_PROY"),
                                              lit("Sólo Proyectos").when(col("TIPO_ACT_PROY") == "2").otherwise("Actividades"))) \
       .withColumn("DEPARTAMENTO_EJECUTORA", concat_ws("-", col("DEPARTAMENTO_EJECUTORA"), col("DEPARTAMENTO_EJECUTORA_NOMBRE"))) \
       .withColumn("PROVINCIA_EJECUTORA", concat_ws("-", col("PROVINCIA_EJECUTORA"), col("PROVINCIA_EJECUTORA_NOMBRE"))) \
       .withColumn("DISTRITO_EJECUTORA", concat_ws("-", col("DISTRITO_EJECUTORA"), col("DISTRITO_EJECUTORA_NOMBRE"))) \
       .withColumn("SEC_EJEC", concat_ws("-", col("SEC_EJEC"), col("EJECUTORA_NOM"))) \
       .withColumn("RUBRO", concat_ws("-", col("RUBRO"), col("RUBRO_NOMBRE"))) \
       .withColumn("FUENTE_FINANC", concat_ws("-", col("FUENTE_FINANC"), col("FUENTE_FINANC_NOMBRE"))) \
       .withColumn("FUNCION", concat_ws("-", col("FUNCION"), col("FUNCION_NOMBRE")))

# Realizar la agregación
aggregated_df = df.groupBy("ANO_EJE", "TIPO_GOBIERNO", "SECTOR", "TIPO_ACT_PROY",
                           "DEPARTAMENTO_EJECUTORA", "PROVINCIA_EJECUTORA", "DISTRITO_EJECUTORA", "SEC_EJEC",
                           "RUBRO", "FUENTE_FINANC", "FUNCION") \
                  .agg(spark_sum("MONTO_PIA").alias("MONTO_PIA"),
                       spark_sum("MONTO_PIM").alias("MONTO_PIM"),
                       spark_sum("MONTO_CERTIFICADO").alias("MONTO_CERTIFICADO"),
                       spark_sum("MONTO_COMPROMETIDO_ANUAL").alias("MONTO_COMPROMETIDO_ANUAL"),
                       spark_sum("MONTO_COMPROMETIDO").alias("MONTO_COMPROMETIDO"),
                       spark_sum("MONTO_DEVENGADO").alias("MONTO_DEVENGADO"),
                       spark_sum("MONTO_GIRADO").alias("MONTO_GIRADO"))

# Guardar resultado como archivo CSV
output_path = "processed_data.csv"
aggregated_df.coalesce(1).write.csv(output_path, header=True)

print(f"Data processed and saved to {output_path}")
