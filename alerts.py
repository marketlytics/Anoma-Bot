from email.mime import multipart
import io
import smtplib
import traceback
import pandas as pd
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
import json
import dataframe_image as dfi
from io import BytesIO


class Email:
    def __init__(self, project, test, anomalies_df):
        self.anomalies_df = anomalies_df
        self.project = project
        self.test = test
        self.message = f"This is to notify you that an anomaly has been detected " + \
        f"in {self.project} for {self.test}\n\n"

    def _export_csv(self, df):
        with io.StringIO() as buffer:
            df.to_csv(buffer, index=False)
            return buffer.getvalue()

    def _get_email_content(self, receivers):
        gmail_user = "<email>"
        gmail_password = "<password>" # to be changed after considering

        body = self.message

        multipart = MIMEMultipart()

        multipart["from"] = gmail_user
        multipart["to"] = receivers

        attachment = MIMEApplication(self._export_csv(self.anomalies_df))
        attachment['Content-Disposition'] = 'attachment; filename="{}"'.format('anomabot_update.csv')
        multipart.attach(attachment)
        multipart.attach(MIMEText(body, 'plain'))

        return gmail_user, gmail_password, multipart

    def send_email(self, receivers):
        try:            
            user, password, multipart = self._get_email_content(receivers)

            server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
            server.ehlo()
            server.login(user, password)
            server.sendmail(user, receivers, multipart.as_string())
            server.close()
        except:
            print(f"{traceback.format_exc()}")


class Slack:
    def __init__(self):
        self.webhook = "<slack webhook url>"

    def df_to_image_buffer(self, df):
        buffer = BytesIO()
        dfi.export(df, buffer, table_conversion='matplotlib', max_rows=10)
        buffer.seek(0)
        return buffer

    def upload_image_to_gitlab(self, project_id, filename, buffer_image, gitlab_token):
        try:
            url = 'https://gitlab.com/api/v4/projects/{0}/uploads'.format(project_id)
            headers = {'PRIVATE-TOKEN': gitlab_token}
            files = {'file': (filename, buffer_image, 'text/plain')}
            r = requests.post(url, headers=headers, files=files)
            print(r)
            # while r.status_code not in [200 ,201]:
            #     time.sleep(1)
            return r.json()
        except Exception as e:
            print("error in upload_image_to_gitlab()")
            print("Error: ", str(e))


    def send_message_via_webhook(self, message, image):
        if image is None:
            body = {
                "text": message, 
            }
        else:
            body = {
                "text": message, 
                "attachments": [{
                    "image_url": image, 
                    "text": "Attached Image"
                }]
            }

        r = requests.post(
            self.webhook, 
            data=json.dumps(body), 
            headers={'Content-Type': 'application/json'}
        )

        return r
