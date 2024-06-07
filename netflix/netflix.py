from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, desc, explode, split, year, max, min, floor

# Initialiser Spark
spark = SparkSession.builder.appName("NetflixMovies").getOrCreate()

# Charger les données Netflix
df_netflix = spark.read.option("header", "true").csv("netflix_titles.csv")

# Afficher la structure du dataset
df_netflix.printSchema()

# Lister les réalisateurs les plus prolifiques
df_directors = df_netflix.groupBy("director").count().orderBy("count", ascending=False)
df_directors.show()

# Afficher en pourcentages les pays dans lesquels les films/séries ont été produits
total_count = df_netflix.count()
df_country_counts = df_netflix.groupBy("country").count()
df_country_percentages = df_country_counts.withColumn("percentage", col("count") / total_count * 100)
df_country_percentages.orderBy(desc("percentage")).show()

# Durée moyenne des films, film le plus long, film le plus court
df_movies = df_netflix.filter(df_netflix.type == "Movie")
df_movies.select(avg("duration").alias("avg_duration")).show()
df_movies.select(max("duration").alias("max_duration"), min("duration").alias("min_duration")).show()

# Durée moyenne des films par intervalles de 2 ans
df_movies = df_movies.withColumn("year", col("release_year").cast("int"))
df_movies = df_movies.withColumn("year_interval", (floor(df_movies.year / 2) * 2))
df_movies.groupBy("year_interval").agg(avg("duration").alias("avg_duration")).orderBy("year_interval").show()

# Duo réalisateur-acteur le plus collaboratif
df_exploded = df_netflix.withColumn("actor", explode(split(df_netflix.cast, ", ")))
df_collaborations = df_exploded.groupBy("director", "actor").count().orderBy(desc("count"))
df_collaborations.show()
