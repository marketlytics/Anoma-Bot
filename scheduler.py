import traceback
from google.cloud import scheduler_v1
from googleapiclient import discovery
from utils import Utils
import json


class Scheduler:
    def __init__(self, read_from_config_json=False):
        self.read_from_config_json = read_from_config_json
        self.utils = Utils()
        self.credentials = self.utils.get_scheduler_credentials_with_scopes(read_from_local_service_account=True)
        self.location = "us-central1"
        self.project_id = "marketlytics-dataware-house"
        self.cloud_function_url = "https://us-central1-marketlytics-dataware-house.cloudfunctions.net/generalized-anomaly-bot/get_anomalies?test_id="
        self.parent = f"projects/{self.project_id}/locations/{self.location}"
        self.scheduler_service = discovery.build(
            'cloudscheduler', 
            'v1', 
            credentials=self.credentials
        )

        self.cloud_scheduler_client = scheduler_v1.CloudSchedulerClient(
            credentials=self.credentials
        )

        self.sheets_df = self.utils.get_sheet_as_df(
            self.credentials, 
            'queries'
        )

        config_file = open("config.json", "r")

        self.config = json.load(config_file)

        config_file.close()

    def _get_job_params(self, test_id):
        sheets_sliced_df = self.sheets_df[self.sheets_df['test_id'] == str(test_id)]

        if sheets_sliced_df.shape[0] > 0:
            return {
                'project_id': sheets_sliced_df['project_name'].iloc[0], 
                'name': sheets_sliced_df['test_name'].iloc[0], 
                'target': {"uri": self.cloud_function_url + str(test_id)}, 
                'schedule': sheets_sliced_df['cron_schedule'].iloc[0], 
                'timezone': sheets_sliced_df['timezone'].iloc[0]
            }
        
        return None

    def _does_job_need_to_be_updated(self, orig_job, new_job_params):
        job_name = self.parent + "/jobs/" + new_job_params["name"]

        if (orig_job["name"] == job_name) and (orig_job["schedule"] == new_job_params["schedule"]) and \
            (orig_job["timeZone"] == new_job_params["timezone"]):
            return False
        
        return True

    def does_job_exist(self, job_name):
        try:
            return self.scheduler_service.projects().locations().jobs().get(
                name=self.parent + "/jobs/" + job_name
            ).execute()
        except:
            return False

    def create_job(self, job_params, parent, job_name):
        job = {
            'name': job_name, 
            'http_target': job_params['target'], 
            'schedule': job_params['schedule'], 
            'time_zone': job_params['timezone']
        }

        return self.scheduler_service.projects().locations().jobs().create(
            parent=parent, 
            body=job
        ).execute()

    def update_job(self, job_params, job_name):
        job = {
            'http_target': job_params['target'], 
            'schedule': job_params['schedule'], 
            'time_zone': job_params['timezone']
        }

        return self.scheduler_service.projects().locations().jobs().patch(
            name=job_name, 
            body=job
        ).execute()

    def manage_job_creation(self, job_exists, job_params):
        job_name = self.parent + '/jobs/' + job_params['name']

        if not job_exists:
            print(f"Job {job_params['name']} doesn't exist already, creating new job...")
            return self.create_job(job_params, self.parent, job_name)
        else:
            print(f"Job {job_params['name']} already exists, updating job...")
            return self.update_job(job_params, job_name)

    def check_jobs(self):
        if not self.read_from_config_json:
            for i in range(self.sheets_df.shape[0]):
                test_id = self.sheets_df['test_id'].iloc[i]
                job_params = self._get_job_params(test_id)

                print(f"Working for test id {test_id}")

                if job_params is not None:
                    job = self.does_job_exist(job_params["name"])
                    if not job:
                        self.manage_job_creation(False, job_params)
                    elif self._does_job_need_to_be_updated(job, job_params):
                        self.manage_job_creation(True, job_params)
                    else:
                        print(f"Job {job_params['name']} already exists with same configuration")
                else:
                    print("...")
        else:
            for con in self.config["tests"]:
                test_id = con["test_id"]
                job_params = {
                    'project_id': con['project_name'], 
                    'name': con['test_name'], 
                    'target': {"uri": self.cloud_function_url + str(test_id)}, 
                    'schedule': con['cron_schedule'], 
                    'timezone': con['timezone']
                }

                job = self.does_job_exist(job_params["name"])
                
                if not job:
                    self.manage_job_creation(False, job_params)
                elif self._does_job_need_to_be_updated(job, job_params):
                    self.manage_job_creation(True, job_params)
                else:
                    print(f"Job {job_params['name']} already exists with same configuration")
