import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    LongType,
)
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast


# ============================================================
# 0. Création de la SparkSession
# ============================================================

spark = (
    SparkSession.builder
    .appName("Projet Spark - Pipeline MovieLens")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 1. Chemins
# ============================================================

ratings_path = "data/datasets/ml-latest-small/ratings.csv"
movies_path = "data/datasets/ml-latest-small/movies.csv"

silver_ratings_path = "output/silver/ratings"
silver_movies_path = "output/silver/movies"

gold_top_movies_path = "output/gold/top_movies"
gold_genres_path = "output/gold/genres_popularity"
gold_top_by_genre_path = "output/gold/top_by_genre"
gold_users_activity_path = "output/gold/users_activity"

# ============================================================
# 2. Schémas explicites
# ============================================================

ratings_schema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True),
])

movies_schema = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True),
])


# ============================================================
# 3. Ingestion bronze : lecture CSV avec schéma explicite
# ============================================================

# Lecture des fichiers bruts MovieLens avec un schéma explicite.
# Cela permet d’éviter que Spark devine les types automatiquement.

ratings_raw = (
    spark.read
    .option("header", True)
    .schema(ratings_schema)
    .csv(ratings_path)
)

movies_raw = (
    spark.read
    .option("header", True)
    .schema(movies_schema)
    .csv(movies_path)
)

print("\n=== Schéma ratings brut ===")
ratings_raw.printSchema()

print("\n=== Schéma movies brut ===")
movies_raw.printSchema()

ratings_raw_count = ratings_raw.count()
movies_raw_count = movies_raw.count()

print("Nombre de lignes ratings brut :", ratings_raw_count)
print("Nombre de lignes movies brut :", movies_raw_count)

print("\n=== Aperçu ratings brut ===")
ratings_raw.show(5, truncate=False)

print("\n=== Aperçu movies brut ===")
movies_raw.show(5, truncate=False)


# ============================================================
# 4. Nettoyage silver
# ============================================================

# Nettoyage des notes :
# - suppression des doublons
# - suppression des lignes incomplètes
# - conservation uniquement des notes entre 0.5 et 5
# - ajout de l’année pour partitionner la couche silver

ratings_clean = (
    ratings_raw
    .dropDuplicates(["userId", "movieId", "timestamp"])
    .dropna(subset=["userId", "movieId", "rating", "timestamp"])
    .filter((F.col("rating") >= 0.5) & (F.col("rating") <= 5.0))
    .withColumn("rating_year", F.year(F.from_unixtime(F.col("timestamp"))))
)

movies_clean = (
    movies_raw
    .dropDuplicates(["movieId"])
    .dropna(subset=["movieId", "title"])
    .filter(F.col("title") != "")
    .withColumn("genres_array", F.split(F.col("genres"), "\\|"))
)

ratings_clean_count = ratings_clean.count()
movies_clean_count = movies_clean.count()

print("\n=== Nettoyage ===")
print("Ratings supprimés :", ratings_raw_count - ratings_clean_count)
print("Movies supprimés :", movies_raw_count - movies_clean_count)

print("\n=== Aperçu ratings clean ===")
ratings_clean.show(5, truncate=False)

print("\n=== Aperçu movies clean ===")
movies_clean.show(5, truncate=False)


# ============================================================
# 5. Écriture couche silver en Parquet
# ============================================================

# Écriture de la couche silver.
# Les ratings sont partitionnés par année afin de montrer une logique d’organisation des données.

(
    ratings_clean
    .write
    .mode("overwrite")
    .partitionBy("rating_year")
    .parquet(silver_ratings_path)
)

(
    movies_clean
    .write
    .mode("overwrite")
    .parquet(silver_movies_path)
)

print("\nCouche silver écrite en Parquet.")


# ============================================================
# 6. Relecture silver
# ============================================================

ratings = spark.read.parquet(silver_ratings_path)
movies = spark.read.parquet(silver_movies_path)

ratings.cache()
print("Nombre ratings silver :", ratings.count())


# ============================================================
# 7. Analyse 1 - Agrégation : films les mieux notés
# ============================================================

# Analyse 1 :
# On cherche les films les mieux notés, mais uniquement ceux avec au moins 50 notes.
# Le seuil évite de classer trop haut un film noté par très peu d’utilisateurs.

ratings_by_movie = (
    ratings
    .groupBy("movieId")
    .agg(
        F.count("*").alias("nb_notes"),
        F.round(F.avg("rating"), 2).alias("note_moyenne")
    )
    .filter(F.col("nb_notes") >= 50)
)

top_movies = (
    ratings_by_movie
    .join(broadcast(movies), on="movieId", how="inner")
    .select("movieId", "title", "nb_notes", "note_moyenne")
    .orderBy(F.desc("note_moyenne"), F.desc("nb_notes"))
)

print("\n=== Analyse 1 : films les mieux notés avec au moins 50 notes ===")
top_movies.show(10, truncate=False)

(
    top_movies
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(gold_top_movies_path)
)


# ============================================================
# 8. Analyse 2 - Jointure : popularité par genre
# ============================================================

# Analyse 2 :
# Les genres sont séparés avec explode car un film peut appartenir à plusieurs genres.

ratings_movies = (
    ratings
    .join(broadcast(movies), on="movieId", how="inner")
)

genres_analysis = (
    ratings_movies
    .withColumn("genre", F.explode("genres_array"))
    .filter(F.col("genre") != "(no genres listed)")
    .groupBy("genre")
    .agg(
        F.count("*").alias("nb_notes"),
        F.countDistinct("movieId").alias("nb_films"),
        F.round(F.avg("rating"), 2).alias("note_moyenne")
    )
    .orderBy(F.desc("nb_notes"))
)

print("\n=== Analyse 2 : popularité par genre ===")
genres_analysis.show(20, truncate=False)

(
    genres_analysis
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(gold_genres_path)
)


# ============================================================
# 9. Analyse 3 - Window function : top 3 films par genre
# ============================================================

# Analyse 3 :
# La window function permet de créer un classement séparé pour chaque genre.
# On garde ensuite uniquement les trois premiers films de chaque genre.

movies_exploded = (
    movies
    .withColumn("genre", F.explode("genres_array"))
    .filter(F.col("genre") != "(no genres listed)")
)

movie_scores_by_genre = (
    ratings_by_movie
    .join(broadcast(movies_exploded), on="movieId", how="inner")
    .select("genre", "title", "nb_notes", "note_moyenne")
)

window_genre = Window.partitionBy("genre").orderBy(
    F.desc("note_moyenne"),
    F.desc("nb_notes")
)

top_by_genre = (
    movie_scores_by_genre
    .withColumn("rang", F.row_number().over(window_genre))
    .filter(F.col("rang") <= 3)
    .orderBy("genre", "rang")
)

print("\n=== Analyse 3 : top 3 films par genre avec window function ===")
top_by_genre.show(60, truncate=False)

(
    top_by_genre
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(gold_top_by_genre_path)
)

# ============================================================

# Analyse bonus - Utilisateurs les plus actifs

# ============================================================

print("\n=== Analyse bonus : utilisateurs les plus actifs ===")

users_activity = (
    ratings
    .groupBy("userId")
    .agg(
        F.count("*").alias("nb_notes"),
        F.round(F.avg("rating"), 2).alias("note_moyenne_utilisateur")
    )
    .orderBy(F.desc("nb_notes"))
)
users_activity.show(10, truncate=False)

(
    users_activity
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(gold_users_activity_path)
)

# ============================================================
# 10. Optimisation mesurée : jointure sans broadcast vs avec broadcast
# ============================================================

# Optimisation :
# La table movies est petite, donc on teste une jointure avec broadcast.
# Spark peut alors envoyer cette petite table à chaque worker au lieu de faire une jointure classique.

print("\n=== Optimisation : comparaison jointure sans broadcast / avec broadcast ===")

start = time.time()
join_without_broadcast = ratings.join(movies, on="movieId", how="inner")
count_without_broadcast = join_without_broadcast.count()
duration_without_broadcast = time.time() - start

start = time.time()
join_with_broadcast = ratings.join(broadcast(movies), on="movieId", how="inner")
count_with_broadcast = join_with_broadcast.count()
duration_with_broadcast = time.time() - start

print("Jointure sans broadcast - lignes :", count_without_broadcast)
print("Temps sans broadcast :", round(duration_without_broadcast, 3), "secondes")

print("Jointure avec broadcast - lignes :", count_with_broadcast)
print("Temps avec broadcast :", round(duration_with_broadcast, 3), "secondes")

print("\n=== Plan avec broadcast ===")
join_with_broadcast.explain()


# ============================================================
# 11. Exploration au-delà du cours : effet du cache
# ============================================================

print("\n=== Exploration : effet du cache sur un DataFrame réutilisé ===")

ratings_no_cache = spark.read.parquet(silver_ratings_path)

start = time.time()
ratings_no_cache.groupBy("movieId").agg(F.avg("rating")).count()
ratings_no_cache.groupBy("userId").agg(F.avg("rating")).count()
duration_no_cache = time.time() - start

ratings_cached = spark.read.parquet(silver_ratings_path).cache()
ratings_cached.count()

start = time.time()
ratings_cached.groupBy("movieId").agg(F.avg("rating")).count()
ratings_cached.groupBy("userId").agg(F.avg("rating")).count()
duration_cache = time.time() - start

print("Temps sans cache :", round(duration_no_cache, 3), "secondes")
print("Temps avec cache :", round(duration_cache, 3), "secondes")

# ============================================================
# 12. Exploration au-delà du cours : nombre de partitions de shuffle
# ============================================================

print("\n=== Exploration : effet du nombre de partitions de shuffle ===")

for partitions in [4, 8, 16]:
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))

    start = time.time()

    test_partitions = (
        ratings_movies
        .withColumn("genre", F.explode("genres_array"))
        .filter(F.col("genre") != "(no genres listed)")
        .groupBy("genre")
        .agg(
            F.count("*").alias("nb_notes"),
            F.round(F.avg("rating"), 2).alias("note_moyenne")
        )
    )

    test_partitions.count()
    duration = time.time() - start

    print(f"Partitions shuffle = {partitions} | Temps = {round(duration, 3)} secondes")


# ============================================================
# 13. Pause Spark UI
# ============================================================

input("\nSpark UI sur http://localhost:4040 - prenez les captures puis appuyez sur Entrée...")

spark.stop()