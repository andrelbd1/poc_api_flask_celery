import boto3
import logging
import numpy as np
import pandas as pd
from io import BytesIO
from src.exceptions import *
from src.config import settings
from datetime import datetime, date, timedelta 


class Utils:
    
    @classmethod
    def adjust_time(cls, df):
        """Decrease in three hours all datetime values from a dataframe
        :param df: dataframe to be processed
        :return: dataframe processed
        """
        try:
            for column in df.select_dtypes(include=['datetime64']).columns:
                df[column]=pd.to_datetime(df[column])
                df[column]=df[column]-timedelta(hours=3)
                df[column]=df[column].dt.date
            return df
        except Exception as err:
            logging.exception(f"process_report :: something wrong happened in adjust time: {err!r}")
            return None

    @classmethod
    def read_list(cls, src_file) -> list:
        """Read strings separated by lines from a file
        :param src_file: file address 
        :return: list of strings
        """
        with open(src_file, 'r') as file:
            lst = [line.strip() for line in file.readlines()]
        return lst

    @classmethod
    def save_report(cls, dct: dict, name:str) -> str:
        """Save report to S3 as xlxs format
        :param dct: dictionary where keys are the sheet_name and values are the spreadsheet
        :param name: file name
        :return: report location from S3
        """
        try:
            file_buffer=BytesIO()
            with pd.ExcelWriter(file_buffer, engine='xlsxwriter') as writer:
                for label, df in dct.items():
                    df.to_excel(writer, sheet_name=label, index=False)

            s3_resource = boto3.resource('s3',
                                            aws_access_key_id=settings.S3_ACCESS_KEY,
                                            aws_secret_access_key=settings.S3_SECRET_KEY)

            s3_resource.Object(settings.S3_BUCKET, f'{settings.S3_REPORT_FILE_PATH}/{name}').put(Body=file_buffer.getvalue())
            return f's3://{settings.S3_BUCKET}/{settings.S3_REPORT_FILE_PATH}/{name}'

        except Exception as err:
            logging.exception(f"process_report :: something wrong happened to save report: {report}: {err!r}")
            return None


    @classmethod
    def list_reports(cls) -> list:
        try: 
            s3_resource = boto3.resource('s3',
                                    aws_access_key_id=settings.S3_ACCESS_KEY,
                                    aws_secret_access_key=settings.S3_SECRET_KEY)

            bucket = s3_resource.Bucket(settings.S3_IS_BUCKET)

            lst_obt = [obj for obj in bucket.objects.all()]
            return lst_obt
        
        except Exception as err:
            logging.exception(f"process_report :: something wrong happened to list reports: {err!r}")
            return None

    
    @classmethod
    def remove_reports(cls, report):
        try: 
            s3_resource = boto3.resource('s3',
                                    aws_access_key_id=settings.S3_ACCESS_KEY,
                                    aws_secret_access_key=settings.S3_SECRET_KEY)

            s3_resource.Object(settings.S3_BUCKET, settings.S3_REPORT_FILE_PATH+'/'+report).delete()
        
        except Exception as err:
            logging.exception(f"process_report :: something wrong happened to remove report: {err!r}")
            return None

    @classmethod
    def download_reports(cls, report):
        try: 
            client = boto3.client('s3',
                                    aws_access_key_id=settings.S3_ACCESS_KEY,
                                    aws_secret_access_key=settings.S3_SECRET_KEY)

            client.download_file(settings.S3_BUCKET, settings.S3_REPORT_FILE_PATH+'/'+report, report)
        
        except Exception as err:
            logging.exception(f"process_report :: something wrong happened to download report: {err!r}")