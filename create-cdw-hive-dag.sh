# Create a resource
./cde resource create --name cdw-hive-dag 
./cde resource upload --name cdw-hive-dag --local-path cdw-hive-dag.py

# Create Job of “airflow” type and reference the DAG
./cde job delete --name cdw-hive-job
./cde job create --name cdw-hive-job --type airflow --dag-file cdw-hive-dag.py  --mount-1-resource cdw-hive-dag 

#Trigger Airflow job to run
#./cde job run --name cdw-lab8-dag-job 
