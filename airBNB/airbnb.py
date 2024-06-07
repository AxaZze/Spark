from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, desc
import matplotlib.pyplot as plt

# Initialiser Spark
spark = SparkSession.builder.appName("AirbnbLondon").getOrCreate()

# Charger les données
df_airbnb = spark.read.option("header", "true").csv("listings-2.csv")

# Afficher la structure du dataset
df_airbnb.printSchema()

# Nombre de listings par quartier
df_neighbourhood_counts = df_airbnb.groupBy("neighbourhood").count().orderBy("count", ascending=False)
df_neighbourhood_counts.show()

# Types de propriétés disponibles
df_property_type_counts = df_airbnb.groupBy("property_type").count().orderBy("count", ascending=False)
df_property_type_counts.show()

# Prix moyen par nuit par quartier
df_avg_price_by_neighbourhood = df_airbnb.groupBy("neighbourhood").agg(avg(col("price").cast("float")).alias("average_price")).orderBy("average_price", ascending=False)
df_avg_price_by_neighbourhood.show()

# Distribution des prix
df_airbnb_pd = df_airbnb.select(col("price").cast("float")).toPandas()
plt.hist(df_airbnb_pd["price"], bins=50)
plt.xlabel("Price")
plt.ylabel("Frequency")
plt.title("Price Distribution")
plt.show()

# Nombre moyen de nuits disponibles par an par quartier
df_avg_availability_by_neighbourhood = df_airbnb.groupBy("neighbourhood").agg(avg("availability_365").alias("average_availability")).orderBy("average_availability", ascending=False)
df_avg_availability_by_neighbourhood.show()

# Score moyen des avis par quartier
df_avg_review_score_by_neighbourhood = df_airbnb.groupBy("neighbourhood").agg(avg("review_scores_rating").alias("average_review_score")).orderBy("average_review_score", ascending=False)
df_avg_review_score_by_neighbourhood.show()

# Nombre de listings par type de chambre
df_room_type_counts = df_airbnb.groupBy("room_type").count().orderBy("count", ascending=False)
df_room_type_counts.show()

# Hôtes les plus prolifiques
df_top_hosts = df_airbnb.groupBy("host_id", "host_name").count().orderBy("count", ascending=False).limit(10)
df_top_hosts.show()
