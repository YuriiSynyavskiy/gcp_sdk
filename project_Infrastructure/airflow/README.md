To spin up the Airflow instance:

Go to Compute Engine > VM instances choose the airflow-dna and click > START/RESUME

Wait ~1-2 min and click > SSH.

In terminal please run these commands:

export AIRFLOW_HOME=/home/andrii_skyba/airflow-dna
cd /home/andrii_skyba/airflow-dna
conda activate /home/andrii_skyba/miniconda3/envs/airflow-dna

Together:
airflow scheduler >> scheduler.log &
airflow webserver -p 8080 >> webserver.log &
