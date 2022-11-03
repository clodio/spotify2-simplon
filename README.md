# spotify2-simplon

## Brief

### Contexte du projet

Votre équipe a produit un MVP permettant de suivre l’évolution des playlists spotify ainsi que la popularité des artistes qu’elles contiennent. Vous êtes chargé d’automatiser dans airflow la récupération quotidienne des données ainsi que leurs mise à disposition dans le data catalogue Hive. Vous devez faire en sorte que le rejeux d’une exécution échouée ne retélécharge pas la donnée mais effectue uniquement les calculs d'entrée sortie d’artiste.

Les données récupéré depuis l'api doivent être archivé dans HDFS.

### Modalités pédagogiques

travail individuel

### Livrables

un dépot github contenant le DAG airflow, les scripts HQL de création de table dans Hive, ainsi que le code de traitement de donnée si il n'est pas directement dans le DAG airflow.

## TODO non réalisé

* transformer en parquet les datas
* faire l'init auto de hive
* faire l'init d'un fichier playlist
* revoir les emplacements des fichiers (pour l'instant dans /tmp)
* récupérer les tokens spotify de l'environnement et pas en dur

## Prerequis

### Remplacer les valeurs des tokens spotify dans le fichier airflow

cf. https://developer.spotify.com/dashboard/applications

export SPOTIFY_CLIENT_ID=xxxx;
export SPOTIFY_CLIENT_SECRET=xxxxx;

### Lancer hadoop sur le serveur

```bash
start-hadoop.sh
```

### Lancer hive sur le serveur


### Lancer les commandes hives

A creuser :

* executer un fichier hive depuis le terminal "hive -f /mon/fichier.hql"
* executer une commande hive depuis le terminal "hive- e 'select * from cities_2022'"

```hive
CREATE SCHEMA spotify;

DROP TABLE spotify.artists;

CREATE EXTERNAL TABLE IF NOT EXISTS spotify.artists ( name String, id String, popularity int) PARTITIONED BY (year int, month int, day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

# Exemple de load
LOAD DATA INPATH '/spotify/artists/csv/year=2022/month=10/day=14/artists_content.csv' OVERWRITE INTO TABLE spotify.artists PARTITION(year=2022, month=10, day=14);

DROP TABLE spotify.tracks;

CREATE EXTERNAL TABLE IF NOT EXISTS spotify.tracks ( playlist_id String, track_id String, artist_id String) PARTITIONED BY (year int, month int, day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

# Exemple de load

LOAD DATA INPATH '/spotify/tracks/csv/year=2022/month=10/day=14/tracks_content.csv' OVERWRITE INTO TABLE spotify.tracks PARTITION(year=2022, month=10, day=14);

DROP TABLE spotify.in_out;

CREATE EXTERNAL TABLE IF NOT EXISTS spotify.in_out (artist_id String, status String) PARTITIONED BY (year int, month int, day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

LOAD DATA INPATH '/spotify/in_out/csv/year=2022/month=10/day=14/in_out_content.csv' OVERWRITE INTO TABLE spotify.in_out PARTITION(year=2022, month=10, day=14);
```

### Lancer airflow sur le serveur

```bash
airflow airflow standalone
```

## récuperer le projet python spotify

```bash
mkdir -p /home/simplon/
cd /home/simplon/
git clone https://github.com/scauglog/spotify_playlist.git
```

## copier le fichier playlist initial et le airflow

```bash
cd /home/simplon/
git clone https://github.com/clodio/spotify2-simplon
cd ./spotify2-simplon
cp playlist.csv /home/simplon/spotify_playlist/playlist.csv
cp spotify_airflow.py /home/simplon/airflow/dags/spotify_airflow.py
cp artists.sh /home/simplon/airflow/dags/artists.sh
cp tracks.sh /home/simplon/airflow/dags/tracks.sh
cp in_out.sh /home/simplon/airflow/dags/in_out.sh
```

## Lancer le dag de airflow depuis http://localhost:8080/dags/ 
