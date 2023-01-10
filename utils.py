from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.storage.blob import Blob
from apiclient.discovery import build
import json
import pandas as pd
import numpy as np
import os


class Utils:
    def __init__(self):
        self.credentials_scopes = [
            "https://www.googleapis.com/auth/bigquery", 
            "https://www.googleapis.com/auth/spreadsheets.readonly", 
            "https://www.googleapis.com/auth/cloud-platform"
        ]

        self.sheet_id = "<sheet id>"
        self.anomaly_tests_runner_service_acc_path = os.environ.get("ANOMALY_TESTS_RUNNER_SERVICE_ACC_PATH", None)
        self.anomaly_tests_scheduler_service_acc_path = os.environ.get("ANOMALY_TESTS_SCHEDULER_SERVICE_ACC_PATH", None)
        # self.storage_client = storage.Client()
        self.storage_client = None

    def get_credentials_with_scopes(self, read_from_local_service_account=False):
        blob = Blob.from_string(self.anomaly_tests_runner_service_acc_path)
        file = blob.download_as_string(self.storage_client)
        return service_account.Credentials.from_service_account_info(
            json.loads(file),
            scopes=self.credentials_scopes 
        )

    def get_scheduler_credentials_with_scopes(self, read_from_local_service_account=False):
        if not read_from_local_service_account:
            blob = Blob.from_string(self.anomaly_tests_scheduler_service_acc_path)
            file = blob.download_as_string(self.storage_client)
            return service_account.Credentials.from_service_account_info(
                json.loads(file),
                scopes=self.credentials_scopes 
            )
        else:
            return service_account.Credentials.from_service_account_file(
                "scheduler_service_account.json", 
                scopes=self.credentials_scopes
            )

    def construct_query_for_test(self, main_table_name=None, date_column_name=None, dataset_column_name=None, 
                                    dataset_table_column_name=None, entries_column_name=None, test_type=None):
        if test_type == 'data_arrived_or_not':
            query = f"""
                select max({date_column_name}) last_entry_date from 
                {main_table_name}
            """
        elif test_type == "no_of_rows":
            query = f"""
                select count(1) no_of_rows from 
                {main_table_name} where
                {date_column_name} = current_date()
            """
        elif test_type == 'anomaly' and dataset_column_name is not None:
            query = f"""
                with orig_table as (
                    select * from {main_table_name} 
                )

                select {date_column_name}, concat({dataset_column_name}, '|', {dataset_table_column_name}) column_to_pivot_on, 
                {entries_column_name} current_day_rows from orig_table
            """
        elif test_type == 'anomaly' and dataset_column_name is None:
             query = f"""
                select {date_column_name}, {dataset_table_column_name} column_to_pivot_on, 
                {entries_column_name} current_day_rows from {main_table_name}
            """

        return query

    def get_query_for_test(self, test_name):
        fp = open("queries/" + test_name + ".sql", "r")
        query = fp.read()
        fp.close()

        return query


    def get_sheet_as_df(self, credentials, range):
        service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)
        sheets = service.spreadsheets()

        sheet_values = sheets.values().get(
            spreadsheetId=self.sheet_id, 
            range=range
        ).execute()["values"]

        return pd.DataFrame(
            sheet_values[1:], 
            columns=sheet_values[0]
        )

    def get_query_results_as_df(self, credentials, query_script, project_id):
        bq_client = bigquery.Client(
            credentials=credentials, 
            project=project_id
        )

        query_results = bq_client.query(query_script).result()

        rows = [dict(result) for result in query_results]

        query_result_df = pd.DataFrame(rows)    

        # if query_result_df.shape[0] > 0:
        #     query_result_df['dataset|table'] = query_result_df[['dataset_id', 'table_name']].agg('|'.join, axis=1)

        return query_result_df

    def get_last_anomalous(self, column, threshold):
        if sum([str(x) != 'nan' for x in column[0:len(column) - 1]]) < 3:
            return None

        q1, q2 = np.percentile([x for x in column[0:len(column) - 1] if str(x) != 'nan'], [threshold, 100 - threshold])
        iqr = q2 - q1

        last = column[-1]

        if str(last) != 'nan' and ((last < (q1 - (1.5 * iqr))) or (last > (q2 + (1.5 * iqr)))):
            return last, q1, q2
        
        return None

    def check_anomaly(self, anomalies):
        nans_filtered_anomalies = anomalies.iloc[[str(x) != 'nan' and str(x) != 'None' for x in anomalies]]

        if len(nans_filtered_anomalies) > 0:
            results_df = nans_filtered_anomalies.to_frame()
            results_df = results_df.reset_index()

            quartiles_df = pd.DataFrame()

            for i, row in results_df.iterrows():
                quartiles_df = quartiles_df.append(pd.DataFrame({
                    'dataset|table': [row['column_to_pivot_on']], 
                    "today's rows": [row[0][0]], 
                    "10%": [row[0][1]], 
                    "90%": [row[0][2]]
                }))

            quartiles_df = quartiles_df.reset_index(drop=True)
            # quartiles_df[['dataset', 'table']] = quartiles_df['dataset|table'].str.split("|", expand=True)

            return quartiles_df
        else:
            print(f"No anomalies found")
            return pd.DataFrame()
