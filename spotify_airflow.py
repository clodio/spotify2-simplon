# The DAG object; we'll need this to instantiate a DAG

import csv
import airflow
import random
import os
import shutil
import pandas as pd
from os.path import exists
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task


from datetime import datetime, timedelta
from textwrap import dedent

# 'start_date': (datetime.now() - timedelta(days=1)).replace(hour=2),
default_args = {
  'owner': 'Simplon Team',
  'depends_on_past': False,
  'email': ['claude.seguret@gmail.com'],
  'email_on_failure': True,
  'email_on_retry': False,
  'retries': 0,
  'max_active_runs': 1,
  'retry_delay': timedelta(minutes=15)
}

dag_id = 'start'
dag = DAG('airflow_spotify',
  dag_id,
  default_args=default_args,
  schedule_interval=timedelta(days=1),
  start_date = datetime(2022, 10, 14, 9, 0, 0)
)

start = DummyOperator(task_id='start_id',  dag=dag)

get_tracks = BashOperator(task_id='get_tracks_id',  bash_command='''
cd /home/simplon/spotify_playlist/spotify_playlist;
export SPOTIFY_CLIENT_ID=TO_CHANGE;
export SPOTIFY_CLIENT_SECRET=TO_CHANGE;
python3 get_tracks.py;
cp tracks* /tmp/;
''', dag=dag)

get_artists = BashOperator(task_id='get_artists_id',  bash_command='''
cd /home/simplon/spotify_playlist/spotify_playlist;
export SPOTIFY_CLIENT_ID=TO_CHANGE;
export SPOTIFY_CLIENT_SECRET=TO_CHANGE;
python3 get_artists.py;
cp artists* /tmp/;
''', dag=dag)


def calculate_in_out():

  previous_tracks_file = "/tmp/tracks_previous.csv"
  current_tracks_file = "/tmp/tracks_current.csv"

  if not exists(previous_tracks_file):
    f = open(previous_tracks_file, "a")
    f.write("playlist_id,track_id,artist_id,date" + '\n')
    f.close()

  current_tracks = pd.read_csv(current_tracks_file)
  previous_tracks = pd.read_csv(previous_tracks_file)

  previous_tracks_unique = previous_tracks.drop_duplicates(subset=['track_id'])
  current_tracks_unique = current_tracks.drop_duplicates(subset=['track_id'])
  outer = previous_tracks_unique.merge(current_tracks_unique, how='outer', indicator=True)
  
  removed_artist = outer[(outer._merge=='left_only')].drop('_merge', axis=1)
  removed_artist["status"] = "out"
  new_artist = outer[(outer._merge=='right_only')].drop('_merge', axis=1)
  new_artist["status"] = "in"
  
  in_out  = pd.concat([removed_artist, new_artist])[['artist_id','date','status']]

  in_out.to_csv('/tmp/in_out.csv', index=False)

  # Remplacement du previous par le new suite à la fin de traitement pour gerer le in_out
  os.unlink(previous_tracks_file)
  shutil.copyfile(current_tracks_file, previous_tracks_file)
  
  return True

calculate_in_out = PythonOperator(task_id='calculate_in_out_id',  python_callable=calculate_in_out, dag=dag)

send_tracks = BashOperator(task_id='send_tracks_id',  bash_command='/home/simplon/airflow/dags/tracks.sh ', dag=dag)

send_artists = BashOperator(task_id='send_artists_id',  bash_command='/home/simplon/airflow/dags/artists.sh ', dag=dag)

send_in_out = BashOperator(task_id='send_in_out_id',  bash_command='/home/simplon/airflow/dags/in_out.sh ', dag=dag)

clean = BashOperator(task_id='clean_id',  bash_command='echo "que faut-il nettoyer, les fichiers conserves pour debug"', dag=dag)

# worflow simplifié pour éviter les locks du à la config de airflow
start >> get_tracks >> send_tracks >> get_artists >> send_artists >> calculate_in_out >> send_in_out >> clean
