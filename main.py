from datetime import datetime
from utils import Utils
from alerts import Slack
import os
import pandas as pd

def get_anomalies(request):
    query_params = request.args

    print(f"Query Params: {query_params}")

    test_id = query_params.get("test_id", None)

    git_project_id = os.environ.get("GIT_PROJECT_ID", None)
    git_token = os.environ.get("GIT_TOKEN", None)
    
    utils = Utils()
    credentials = utils.get_credentials_with_scopes()

    queries_df = utils.get_sheet_as_df(credentials, "queries")
    sliced_queries_df = queries_df[queries_df['test_id'] == str(test_id)]

    if sliced_queries_df.shape[0] < 1:
        return "No test available with given test_id"

    test_type = sliced_queries_df['test_type'].iloc[0]
    test_name = sliced_queries_df['test_name'].iloc[0]
    slack_member_id = sliced_queries_df['slack_member_id'].iloc[0]

    if test_type == 'anomaly':
        query = utils.construct_query_for_test(
            main_table_name=sliced_queries_df['main_table_name'].iloc[0], 
            date_column_name=sliced_queries_df['date_column_name'].iloc[0],
            dataset_column_name=sliced_queries_df['dataset_column_name'].iloc[0],
            dataset_table_column_name=sliced_queries_df['dataset_table_column_name'].iloc[0],
            entries_column_name=sliced_queries_df['entries_column_name'].iloc[0], 
            test_type=test_type
        )

        query_result_df = utils.get_query_results_as_df(
            credentials=credentials, 
            query_script=query, 
            project_id=sliced_queries_df['project_name'].iloc[0]
        )

        if "column_to_pivot_on" in list(query_result_df.columns):
            pivot_df = query_result_df.pivot_table(
                index='date', 
                columns = 'column_to_pivot_on',
                values = 'current_day_rows'
            )
        else:
            pivot_df = query_result_df

        anomalies = pivot_df.apply(
            utils.get_last_anomalous, 
            axis=0, 
            threshold=float(sliced_queries_df['threshold'].iloc[0])
        )

        quartiles_df = utils.check_anomaly(anomalies)

        slack = Slack()

        if quartiles_df.shape[0] == 0:
            slack.send_message_via_webhook(
                f"Anomly test successfully run for {test_name}, " + \
                "No Anomalies detected", 
                image=None
            )    
        else:
            image_buffer = slack.df_to_image_buffer(quartiles_df)
            jsn = slack.upload_image_to_gitlab(git_project_id, 
                "Test.png",
                image_buffer,
                git_token
            )

            print('Image Url: ' + 'https://gitlab.com'+jsn['full_path'])

            slack.send_message_via_webhook(
                f"Hey, <@{slack_member_id}>! Anomly test successfully run for {test_name}, " + \
                "Anomalies detected, run query for the test to see more", 
                image='https://gitlab.com'+jsn['full_path']
            )  

        return "Executed successfully"
    
    elif test_type == 'data_arrived_or_not':
        query = utils.construct_query_for_test(
            main_table_name=sliced_queries_df['main_table_name'].iloc[0],
            date_column_name=sliced_queries_df['date_column_name'].iloc[0],
            test_type=test_type
        )

        query_result_df = utils.get_query_results_as_df(
            credentials=credentials, 
            query_script=query, 
            project_id=sliced_queries_df['project_name'].iloc[0]
        )

        slack = Slack()

        print(query_result_df['last_entry_date'].iloc[0])
        print(datetime.today().date().strftime("%Y-%m-%d"))

        if str(query_result_df['last_entry_date'].iloc[0]) == datetime.today().date().strftime("%Y-%m-%d"):
            slack.send_message_via_webhook(
                message=f"Test successfully run for {test_name}, " + \
                "Data has been collected", 
                image=None, 
            )
        else:
            image_buffer = slack.df_to_image_buffer(query_result_df)
            jsn = slack.upload_image_to_gitlab(git_project_id, 
                "Test.png",
                image_buffer,
                git_token
            )

            print('https://gitlab.com'+jsn['full_path'])

            slack.send_message_via_webhook(
                message=f"Hey, <@{slack_member_id}>! Test successfully run for {test_name}, " + \
                "Data has not been collected", 
                image='https://gitlab.com'+jsn['full_path'], 
            )

        return "Executed Successfully"
    elif test_type == "no_of_rows":
        rows = 0
        grouped_query_results_df = pd.DataFrame()
        zero_row_found = False

        for i in range(sliced_queries_df.shape[0]):
            query = utils.construct_query_for_test(
                main_table_name=sliced_queries_df['main_table_name'].iloc[i],
                date_column_name=sliced_queries_df['date_column_name'].iloc[i],
                test_type=test_type
            )

            query_result_df = utils.get_query_results_as_df(
                credentials=credentials, 
                query_script=query, 
                project_id=sliced_queries_df['project_name'].iloc[0]
            )

            print(f"Query {i}: {query_result_df}")

            grouped_query_results_df = grouped_query_results_df.append(pd.DataFrame({
                'dataset/table': [sliced_queries_df['main_table_name'].iloc[i].split('.')[1]], 
                'no_of_rows': [query_result_df['no_of_rows'].iloc[0]]
            }))

            if query_result_df['no_of_rows'].iloc[0] == 0:
                zero_row_found = True

            rows += query_result_df['no_of_rows'].iloc[0]

        print(f"Total Rows: {rows}")
        print(f"Grouped DataFrame: {grouped_query_results_df}")

        slack = Slack()
        
        image_buffer = slack.df_to_image_buffer(grouped_query_results_df)

        jsn = slack.upload_image_to_gitlab(git_project_id, 
            "Test.png",
            image_buffer,
            git_token
        )

        if zero_row_found:
            slack.send_message_via_webhook(
                message=f"Hey, <@{slack_member_id}>! Test successfully run for {test_name}, " + \
                f"Total no. of rows collected today: {rows}", 
                image='https://gitlab.com'+jsn['full_path'], 
            )
        else:
            slack.send_message_via_webhook(
                message=f"Test successfully run for {test_name}, " + \
                f"Total no. of rows collected today: {rows}", 
                image='https://gitlab.com'+jsn['full_path'], 
            )            

        return "Executed Successfully"
    else:
        return "test_type isn't configured correct. Check again!"


if __name__ == '__main__':
    print(get_anomalies(None))
