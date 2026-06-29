Projet Spark — Pipeline MovieLens

Description du projet

Ce projet met en place un pipeline de traitement de données avec PySpark en mode local.

Le pipeline utilise le dataset MovieLens afin de traiter des données de notes de films et de produire plusieurs analyses à partir d’une architecture inspirée des couches bronze, silver et gold.

Dataset utilisé

Le projet utilise les fichiers MovieLens suivants :

* ratings.csv : notes données par les utilisateurs ;
* movies.csv : informations sur les films et leurs genres.

Les données brutes ne sont pas incluses dans le dépôt Git afin d’éviter de versionner les fichiers téléchargés.

Elles peuvent être récupérées avec le script fourni :

bash data/download.sh

Fichier principal

Le code principal du projet se trouve dans :

projet_movielens_pipeline.py

Ce script réalise :

* l’ingestion des données brutes ;
* le nettoyage des données ;
* l’écriture de la couche silver en Parquet ;
* les analyses gold au format CSV ;
* une optimisation avec broadcast join ;
* une exploration du cache et du nombre de partitions de shuffle.

Structure des livrables

.
├── projet_movielens_pipeline.py
├── README_PROJET.md
├── projects/
│   ├── rapport-modele.md
│   └── captures/
│       ├── spark_jobs.png
│       ├── spark_stages.png
│       ├── spark_stage_detail.png
│       ├── spark_tasks.png
│       └── spark_sql_dataframe.png
└── output/
    ├── silver/
    │   ├── ratings/
    │   └── movies/
    └── gold/
        ├── top_movies/
        ├── genres_popularity/
        ├── top_by_genre/
        └── users_activity/

Exécution du pipeline

Activer l’environnement virtuel :

source .venv/bin/activate

Configurer Java si nécessaire :

export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

Lancer le pipeline :

python projet_movielens_pipeline.py

Sorties produites

Le pipeline produit une couche silver au format Parquet :

* output/silver/ratings/
* output/silver/movies/

Il produit également plusieurs sorties gold au format CSV :

* output/gold/top_movies/
* output/gold/genres_popularity/
* output/gold/top_by_genre/
* output/gold/users_activity/

Analyses réalisées

Le projet contient plusieurs analyses :

1. Identification des films les mieux notés avec au moins 50 notes.
2. Analyse de la popularité des genres.
3. Classement des trois meilleurs films par genre avec une window function.
4. Analyse bonus des utilisateurs les plus actifs.

Optimisation et exploration

Une optimisation mesurée a été réalisée avec une comparaison entre une jointure classique et une jointure avec broadcast.

Le projet contient aussi une exploration :

* de l’effet du cache sur un DataFrame réutilisé plusieurs fois ;
* de l’effet du nombre de partitions de shuffle sur une agrégation.

Spark UI

Des captures de la Spark UI sont disponibles dans le dossier :

projects/captures/

Elles documentent les jobs, stages, tasks et requêtes SQL/DataFrame générés par Spark.

Rapport

Le rapport écrit du projet se trouve dans :

projects/rapport-modele.md