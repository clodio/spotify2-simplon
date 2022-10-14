#!/bin/bash


data='in_out'
backup_dir=$(date +"year=%Y/month=%m/day=%d");
import_file_date=$(date +"%Y-%m-%d");

echo '*** Debut traitement des '${data}' du '${import_file_date}

hdfs dfs -rmdir --ignore-fail-on-non-empty /spotify/${data}/csv/${backup_dir};
hdfs dfs -rm /spotify/${data}/csv/${backup_dir}/${data}_content.csv;
hdfs dfs -rm /spotify/${data}/csv/${backup_dir}/${data}_headers.csv;
hdfs dfs -mkdir -p /spotify/${data}/csv/${backup_dir};
echo 'suppression des données du dernier traitement'
rm -f /tmp/${data}.csv;
rm -f /tmp/${data}_headers.csv;
rm -f /tmp/${data}_content.csv;

echo 'Lancement du découpage et suppression de la colonne date car il y a une partition date'
cut -d, -f1 --complement /tmp/"${data}_"${import_file_date}".csv"> /tmp/${data}.csv 
echo 'Separation header et content'
head -n1 /tmp/${data}.csv > /tmp/${data}_headers.csv;
tail -n +2 /tmp/${data}.csv > /tmp/${data}_content.csv;
echo 'Envoi dans hdfs /spotify/'${data}'/csv/'${backup_dir}
hdfs dfs -put /tmp/${data}_content.csv /spotify/${data}/csv/${backup_dir}/${data}_content.csv;
hdfs dfs -put /tmp/${data}_headers.csv /spotify/${data}/csv/${backup_dir}/${data}_headers.csv;

echo '*** Fin traitement des '${data}' du '${import_file_date}
