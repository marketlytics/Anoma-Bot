# Anoma-Bot
The aim of the Anoma bot is to notify data anomalies, the number of rows added in a day, and any data arrived in a day or not for a particular table of the database.  It is developed in such a way that it can be easily integrated with any project without changing much of the codebase.

All the configuration, project ids, tests, and query parameters are configured in a google sheet. This sheet can hold parameters for multiple tests for multiple different projects.

The code is hosted in a cloud function that on getting triggered reads the sheet or config file, picks the specified test parameters, runs it, and reports the results to slack.
![Anoma Bot Architecture](https://user-images.githubusercontent.com/122284087/213253411-7c95dc14-7b6a-4507-a418-52358f2620a4.JPG)

## Prerequisites
### Service Accounts
#### Scheduler Service Account: 
Should be set as an environment variable with name ANOMALY_TESTS_SCHEDULER_SERVICE_ACC_PATH.
This is used for scheduling tests as cloud scheduler jobs.

### Tests Runner Service Account: 
Should be set as an environment variable with name ANOMALY_TESTS_RUNNER_SERVICE_ACC_PATH.
This is used as running those tests scheduled as jobs.
	
### Permissions above service accounts should have: 
Permissions to access Google Cloud Storage
Permissions to schedule Google Cloud Scheduler Jobs
Permissions to Access BigQuery Resources.
Permissions for Google Cloud Functions
	
### Gitlab Repository
This is used to upload the pictures of results of tests, so that can be sent to Slack. 
You need to set two environment variables for this.
GIT_PROJECT_ID: The project ID of the Git Repository where results will be uploaded
GIT_TOKEN: Token that has access to the repository with relevant permissions.

### Google Sheet
Configuration spreadsheet for the tests. You need to change the “self.sheet_id” variable in Utils class’ constructor in the utils.py file. Set this variable to your sheet id.

### Slack Channel Webhook URL
The Project uses a Slack channel webhook to send messages/alerts for tests’ results. Set “self.webhook” variable in Slack class to your webhook url in alerts.py file.

### Steps to configure and test Anoma bot by using Google Sheet
The manual in repo contains the detail for configuration and testing of Anoma bot, please refer to it.

