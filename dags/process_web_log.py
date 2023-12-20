from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import re
import glob
import tarfile
from discord_webhook import DiscordWebhook


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 19),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# DAG definition
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='A web log processing DAG that analyzes a web server log file, extracts the required lines and fields, performs some transformations, and writes to another file with Apache Airflow.',
    schedule_interval=timedelta(days=1),
)


# Path to the directory containing the log file
log_folder = '/Users/anto/airflow'

# Path to save the extracted data
extracted_data_path = os.path.join('/Users/anto/airflow','extracted_data.txt')

# Path to save the transformed data
transformed_data_path = os.path.join('/Users/anto/airflow','transformed_data.txt')

# Path to save the archive
archive_path = os.path.join('/Users/anto/airflow','weblog.tar')

workflow_execution_message_path = os.path.join('/Users/anto/airflow', 'workflow_execution_message.txt')


####################################################################################################

# TASK 1: Create a task to scan for log 
def scan_for_log():
    log_folder_path = os.path.join(log_folder, "log.txt")
    matching_files = glob.glob(log_folder_path)

    if not matching_files:
        print(f"No log file found in {log_folder}")
        return None

    log_file_path = matching_files[0]
    print(f"Found log file: {log_file_path}")
    return log_file_path

# Create a task to scan for log
scan_for_log_task = PythonOperator(
    task_id='scan_for_log',
    python_callable=scan_for_log,
    dag=dag,
)


####################################################################################################

# TASK 2: Create a task to extract data
def extract_data(log_file_path, extracted_data_path):
    log_file_path = os.path.join(log_folder, 'log.txt')
    with open(log_file_path, 'r') as log_file:
        lines = log_file.readlines()
        ip_addresses = [re.search(r'\d+\.\d+\.\d+\.\d+', line).group() for line in lines]

    with open(extracted_data_path, 'w') as extracted_file:
        for ip_address in ip_addresses:
            extracted_file.write(f"{ip_address}\n")

    print(f"Data has been extracted and saved to '{extracted_data_path}'.")

# Create a task to extract data
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    op_args=[log_folder,extracted_data_path],
    dag=dag,
)


####################################################################################################

# TASK 3: Function to transform data
def transform_data(extracted_data_path, transformed_data_path, filter_ip="198.46.149.143"):
    with open(extracted_data_path, 'r') as extracted_file:
        ip_addresses = [line.strip() for line in extracted_file.readlines() if line.strip() != filter_ip]

    with open(transformed_data_path, 'w') as transformed_file:
        for ip_address in ip_addresses:
            transformed_file.write(f"{ip_address}\n")

    print(f"Data has been transformed and saved to '{transformed_data_path}'.")

# Create a task to transform data
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_args=[extracted_data_path, transformed_data_path],
    dag=dag,
)


####################################################################################################

# TASK 4: Create a task to load data
def load_data(transformed_data_path, archive_path):
    with tarfile.open(archive_path, 'w') as tar:
        tar.add(transformed_data_path, arcname=os.path.basename(transformed_data_path))

    print(f"Data has been loaded and archived to '{archive_path}'.")

# Create a task to load data
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_args=[extracted_data_path, transformed_data_path],
    dag=dag,
)    


####################################################################################################

# TASK 5: Create a task to send a message indicating the workflow execution
def send_execution_message():
    print("Workflow executed successfully.")

# Create a task to send an execution message
send_execution_message_task = PythonOperator(
    task_id='send_execution_message',
    python_callable=send_execution_message,
    dag=dag,
)


####################################################################################################

# TASK 6: Create a task to send a message to Discord
def message():
    try:
                # Attempt to import the module
        from discordwebhook import Discord
    except ImportError:
                # Install the module if not found
                os.system("pip install discordwebhook")
                # Retry the import
                from discordwebhook import Discord
    discord = Discord(url="https://discord.com/api/webhooks\
                /1178358802467274914/qqUd_wQhldupanCMQFwR7mRtyWhv\
                Ki14eObOz2adtw3zhci824RXZu_Ot7Bs1xpfjPfl")
    discord.post(content="The workflow was executed successfully!")
send_message_task_to_discord = PythonOperator(
    task_id='send_message',
    python_callable=message,
    dag=dag,
)
  
# Define the workflow sequence of tasks
scan_for_log_task >> extract_data_task
extract_data_task >> transform_data_task
transform_data_task >> load_data_task
load_data_task >> send_execution_message_task
send_execution_message_task >> send_message_task_to_discord
