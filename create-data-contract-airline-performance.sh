# Create a resource
./cde resource create --name data-contract-airline-performance --type files 
./cde resource upload --name data-contract-airline-performance --local-path data-contract-airline-performance.py --local-path data-contract-enforcing.py 
./cde resource describe --name data-contract-airline-performance

# Create Job of “airflow” type and reference the DAG--type files 
./cde job delete --name data-contract-airline-performance
./cde job create --name data-contract-airline-performance --type airflow --dag-file data-contract-airline-performance.py --mount-1-resource data-contract-airline-performance
./cde job update --name data-contract-airline-performance --airflow-file-mount-1-resource data-contract-airline-performance
./cde job describe --name data-contract-airline-performance

#Trigger Airflow job to run
#./cde job run --name cdw-lab8-dag-job 
