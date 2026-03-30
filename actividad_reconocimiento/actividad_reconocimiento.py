# Actividad Practica Nro 1. BigData
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import pandas as pd

## Lectura de datos
query =f'select coin, date , price from `usantoto`.`bigdata`.`coins`'

df = spark.sql(query)

df.show(10)
df.printSchema()
print(f"Total de registros: {df.count()}")
from pyspark.sql import functions as F
 # validacion de valores nulos
df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df.columns
]).show()

# Data frame sin valores nulos
df_clean = (
    df.withColumn("date", F.to_date("date"))
      .withColumn("price", F.col("price").cast("double"))
)
 
df_clean.printSchema()
display(df_clean)

# promedio de precio por moneda
display(
    df.groupBy("coin")
      .avg("price")
)
# Agrupacion de promedio y desviacion estandar por moneda

df.groupBy("coin").agg(
    F.avg("price").alias("mean_value"),
    F.stddev_pop("price").alias("pop_stddev_value")
).show()

# Escritura de datos en formato delta
print("iniciando escritura de datos en formato delta ......")
df_clean.write.mode("overwrite").format("delta").saveAsTable("coins_clean")
print("Escritura de datos en formato delta completado ......")

# revisa historial de cambios
DESCRIBE HISTORY workspace.default.coins_clean;

query_coin="""
SELECT
    coin,
    ROUND(MIN(price), 2) AS precio_minimo,
    ROUND(MAX(price), 2) AS precio_maximo
FROM coins_clean
GROUP BY coin
ORDER BY coin
"""
spark.sql(query_coin).show()

# consultas 
print(' -------  Cantidad Total --------')
query_count='''SELECT COUNT(0) as cantidad_registros FROM coins_clean'''
spark.sql(query_count).show()
print('----- Columnas --------')
query_columns='''SELECT coin, price, date FROM coins_clean'''
spark.sql(query_coin).show()
print('------ Filtro --------')
query_filter=f'''SELECT coin, price, date FROM coins_clean WHERE price > 1000 
limit 10'''
spark.sql(query_filter).show()
print('------ Agregacion --------')
query_aggregation='''SELECT coin as moneda, SUM(price) as SUMA FROM coins_clean  
group by 1 '''
spark.sql(query_aggregation).show()

# visualizacion 
w = Window.partitionBy("coin").orderBy("date")
 
df_variation = (
    df_clean.withColumn("prev_price", F.lag("price").over(w))
            .withColumn(
                "daily_change_pct",
                F.round(((F.col("price") - F.col("prev_price")) / F.col("prev_price")) * 100, 2)
            )
)
 
display(df_variation)
