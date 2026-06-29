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
# Chemins du projet
# ============================================================

RATINGS_PATH = "data/datasets/ml-latest-small/ratings.csv"
MOVIES_PATH = "data/datasets/ml-latest-small/movies.csv"

SILVER_RATINGS_PATH = "output/silver/ratings"
SILVER_MOVIES_PATH = "output/silver/movies"

GOLD_TOP_MOVIES_PATH = "output/gold/top_movies"
GOLD_GENRES_PATH = "output/gold/genres_popularity"
GOLD_TOP_BY_GENRE_PATH = "output/gold/top_by_genre"
GOLD_USERS_ACTIVITY_PATH = "output/gold/users_activity"


# ============================================================
# Schémas explicites
# ============================================================

RATINGS_SCHEMA = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", LongType(), True),
])

MOVIES_SCHEMA = StructType([
    StructField("movieId", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True),
])


def creer_session_spark():
    """Créer la SparkSession du projet."""

    spark = (
        SparkSession.builder
        .appName("Projet Spark - Pipeline MovieLens")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def ingestion(spark):
    """Lire les fichiers bruts MovieLens avec un schéma explicite."""

    print("\n=== Ingestion bronze ===")

    ratings_raw = (
        spark.read
        .option("header", True)
        .schema(RATINGS_SCHEMA)
        .csv(RATINGS_PATH)
    )

    movies_raw = (
        spark.read
        .option("header", True)
        .schema(MOVIES_SCHEMA)
        .csv(MOVIES_PATH)
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

    return ratings_raw, movies_raw, ratings_raw_count, movies_raw_count


def nettoyage(ratings_raw, movies_raw, ratings_raw_count, movies_raw_count):
    """Nettoyer les données et préparer la couche silver."""

    print("\n=== Nettoyage silver ===")

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

    print("Ratings supprimés :", ratings_raw_count - ratings_clean_count)
    print("Movies supprimés :", movies_raw_count - movies_clean_count)

    print("\n=== Aperçu ratings clean ===")
    ratings_clean.show(5, truncate=False)

    print("\n=== Aperçu movies clean ===")
    movies_clean.show(5, truncate=False)

    return ratings_clean, movies_clean


def ecrire_silver(ratings_clean, movies_clean):
    """Écrire la couche silver en Parquet."""

    print("\n=== Écriture silver ===")

    (
        ratings_clean
        .write
        .mode("overwrite")
        .partitionBy("rating_year")
        .parquet(SILVER_RATINGS_PATH)
    )

    (
        movies_clean
        .write
        .mode("overwrite")
        .parquet(SILVER_MOVIES_PATH)
    )

    print("Couche silver écrite en Parquet.")


def lire_silver(spark):
    """Relire la couche silver."""

    print("\n=== Relecture silver ===")

    ratings = spark.read.parquet(SILVER_RATINGS_PATH)
    movies = spark.read.parquet(SILVER_MOVIES_PATH)

    ratings.cache()
    print("Nombre ratings silver :", ratings.count())

    return ratings, movies


def analyse_top_movies(ratings, movies):
    """Analyse 1 : films les mieux notés avec un minimum de 50 notes."""

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

    return top_movies, ratings_by_movie


def analyse_genres(ratings, movies):
    """Analyse 2 : popularité des genres."""

    ratings_movies = ratings.join(broadcast(movies), on="movieId", how="inner")

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

    return genres_analysis, ratings_movies


def analyse_top_par_genre(ratings_by_movie, movies):
    """Analyse 3 : top 3 films par genre avec une window function."""

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

    return top_by_genre


def analyse_utilisateurs(ratings):
    """Analyse bonus : utilisateurs les plus actifs."""

    users_activity = (
        ratings
        .groupBy("userId")
        .agg(
            F.count("*").alias("nb_notes"),
            F.round(F.avg("rating"), 2).alias("note_moyenne_utilisateur")
        )
        .orderBy(F.desc("nb_notes"))
    )

    print("\n=== Analyse bonus : utilisateurs les plus actifs ===")
    users_activity.show(10, truncate=False)

    return users_activity


def ecrire_gold(top_movies, genres_analysis, top_by_genre, users_activity):
    """Écrire les résultats gold en CSV."""

    print("\n=== Écriture gold ===")

    sorties = [
        (top_movies, GOLD_TOP_MOVIES_PATH),
        (genres_analysis, GOLD_GENRES_PATH),
        (top_by_genre, GOLD_TOP_BY_GENRE_PATH),
        (users_activity, GOLD_USERS_ACTIVITY_PATH),
    ]

    for dataframe, chemin in sorties:
        (
            dataframe
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(chemin)
        )

        print("Résultat écrit :", chemin)


def optimisation_broadcast(ratings, movies):
    """Comparer une jointure classique avec une jointure broadcast."""

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


def exploration_cache(spark):
    """Mesurer l'effet du cache sur un DataFrame réutilisé plusieurs fois."""

    print("\n=== Exploration : effet du cache sur un DataFrame réutilisé ===")

    ratings_no_cache = spark.read.parquet(SILVER_RATINGS_PATH)

    start = time.time()
    ratings_no_cache.groupBy("movieId").agg(F.avg("rating")).count()
    ratings_no_cache.groupBy("userId").agg(F.avg("rating")).count()
    duration_no_cache = time.time() - start

    ratings_cached = spark.read.parquet(SILVER_RATINGS_PATH).cache()
    ratings_cached.count()

    start = time.time()
    ratings_cached.groupBy("movieId").agg(F.avg("rating")).count()
    ratings_cached.groupBy("userId").agg(F.avg("rating")).count()
    duration_cache = time.time() - start

    print("Temps sans cache :", round(duration_no_cache, 3), "secondes")
    print("Temps avec cache :", round(duration_cache, 3), "secondes")


def exploration_partitions_shuffle(spark, ratings_movies):
    """Comparer plusieurs valeurs de spark.sql.shuffle.partitions."""

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


def main():
    """Orchestrer tout le pipeline Spark."""

    spark = creer_session_spark()

    ratings_raw, movies_raw, ratings_raw_count, movies_raw_count = ingestion(spark)

    ratings_clean, movies_clean = nettoyage(
        ratings_raw,
        movies_raw,
        ratings_raw_count,
        movies_raw_count,
    )

    ecrire_silver(ratings_clean, movies_clean)

    ratings, movies = lire_silver(spark)

    top_movies, ratings_by_movie = analyse_top_movies(ratings, movies)
    genres_analysis, ratings_movies = analyse_genres(ratings, movies)
    top_by_genre = analyse_top_par_genre(ratings_by_movie, movies)
    users_activity = analyse_utilisateurs(ratings)

    ecrire_gold(
        top_movies,
        genres_analysis,
        top_by_genre,
        users_activity,
    )

    optimisation_broadcast(ratings, movies)
    exploration_cache(spark)
    exploration_partitions_shuffle(spark, ratings_movies)

    input("\nSpark UI sur http://localhost:4040 - prenez les captures puis appuyez sur Entrée...")

    spark.stop()


if __name__ == "__main__":
    main()
