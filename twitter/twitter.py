from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, desc, to_date, concat, lit, floor, date_format

# Initialiser Spark
spark = SparkSession.builder.appName("TrumpTweets").getOrCreate()

# Charger les données des tweets de Trump
df_trump_tweets = spark.read.option("header", "true").csv("trump.csv")

# Afficher la structure du dataset
df_trump_tweets.printSchema()

# Afficher quelques lignes du dataset pour vérification
df_trump_tweets.show(5)

# Calculer le nombre total de tweets
total_tweets = df_trump_tweets.count()

# 1. Les 7 comptes que Donald Trump insulte le plus
df_mentions = df_trump_tweets.withColumn("mention", explode(split(col("target"), ", ")))
df_insults_per_account = df_mentions.groupBy("mention").count().orderBy(desc("count")).limit(7)
df_insults_per_account = df_insults_per_account.withColumn("percentage", (col("count") / total_tweets) * 100)
df_insults_per_account.show()

# 2. Les insultes que Donald Trump utilise le plus
df_insults = df_trump_tweets.withColumn("insult", explode(split(col("insult"), ", ")))
df_insults_count = df_insults.groupBy("insult").count().orderBy(desc("count"))
df_insults_count = df_insults_count.withColumn("percentage", (col("count") / total_tweets) * 100)
df_insults_count.show()

# 3. L'insulte que Donald Trump utilise le plus pour Joe Biden
df_biden_insults = df_trump_tweets.filter(col("tweet").contains("Joe Biden")).withColumn("insult", explode(split(col("insult"), ", ")))
df_biden_insults_count = df_biden_insults.groupBy("insult").count().orderBy(desc("count"))
df_biden_insults_count.show(1)

# 4. Combien de fois a-t-il tweeté le mot "Mexico"? Le mot "China"? Le mot "coronavirus"?
mexico_count = df_trump_tweets.filter(col("tweet").contains("Mexico")).count()
china_count = df_trump_tweets.filter(col("tweet").contains("China")).count()
coronavirus_count = df_trump_tweets.filter(col("tweet").contains("coronavirus")).count()
print(f"Tweets with 'Mexico': {mexico_count} ({(mexico_count / total_tweets) * 100:.2f}%)")
print(f"Tweets with 'China': {china_count} ({(china_count / total_tweets) * 100:.2f}%)")
print(f"Tweets with 'coronavirus': {coronavirus_count} ({(coronavirus_count / total_tweets) * 100:.2f}%)")

# 5. Le nombre de tweets par période de 6 mois
df_trump_tweets = df_trump_tweets.withColumn("date", to_date(col("date")))

df_trump_tweets = df_trump_tweets.withColumn("year", date_format(col("date"), "yyyy"))
df_trump_tweets = df_trump_tweets.withColumn("month", date_format(col("date"), "MM").cast("int"))
df_trump_tweets = df_trump_tweets.withColumn("half", (floor((col("month")-1)/6)*6 + 1).cast("string"))
df_trump_tweets = df_trump_tweets.withColumn("period", concat(col("year"), lit("-"), col("half").substr(0,2)))

df_tweets_per_period = df_trump_tweets.groupBy("period").count().orderBy("period")
df_tweets_per_period.show()
