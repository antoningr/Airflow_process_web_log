# Airflow_process_web_log


## Overview
This project aims to create a simple Apache Airflow workflow named "process_web_log" for analyzing a web server log file. The workflow involves tasks such as scanning for the log file, extracting relevant data, transforming the data, loading it into a tar file, and sending a notification about the workflow execution. The workflow is designed to run daily.


## Workflow Tasks
1. Define the DAG

Create a DAG named "process_web_log" that runs daily.

2. Scan for Log

Create a task named "scan_for_log" that scans a folder named "the_logs" for a file named "log.txt." This task triggers the rest of the workflow.

3. Extract Data

Create a task named "extract_data" to extract the 'ipaddress' field from the web server log file and save it into a file named "extracted_data.txt."

4. Transform Data

Create a task named "transform_data" to filter out occurrences of 'ipaddress' 198.46.149.143 from "extracted_data.txt" and save the output to a file named "transformed_data.txt."

5. Load Data

Create a task named "load_data" to archive the file "transformed_data.txt" into a tar file named "weblog.tar."

6. Notify Workflow Execution

Add an additional task after the last task to send a message indicating that the workflow was executed. The message can be sent to a Discord channel.


## Workflow Execution and Testing
Test Individual Tasks:

Run each task independently to ensure they work as expected.
Document any issues or observations during individual task runs.

Test Workflow:

Run the entire workflow and verify that it completes successfully.
Document any issues or observations during the workflow run.

Monitor Workflow:

Trigger and run the workflow multiple times to monitor its behavior.
Document any findings or observations during the monitoring process.


## Files
process_web_log.py: Contains the DAG definition and task configurations.


## Usage
1. Install Apache Airflow.
2. Copy the contents of process_web_log.py to your DAGs folder.
3. Trigger the DAG using Airflow's UI or command line.


## Implementation
1. DAG Definition

The DAG is defined in the file named "process_web_log.py." Refer to the code snippets in the report for details on how the tasks are sequenced.

2. Task Execution

The tasks are executed using Apache Airflow's scheduler and executor. Ensure that Airflow is properly configured, and the required dependencies are installed.


## Discord for notifying workflow execution
The additional task for notifying workflow execution can be implemented using Airflow's operators or external tools such as Discord webhooks.


## Conclusion
This project provides a comprehensive workflow for processing web server log files using Apache Airflow. The defined tasks, sequencing, and notifications contribute to a robust and efficient data processing pipeline.



Note: This README provides a general overview. For detailed implementation, refer to the code snippets and comments in the process_web_log.py file.
