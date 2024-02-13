import uuid
import json
import time
import logging
import numpy as np
import pandas as pd
from typing import Tuple
from celery.exceptions import SoftTimeLimitExceeded
from datetime import datetime, date
from src import celery
from src.config import settings
from src.exceptions import *
# from src.common.Utils import save_report
# from src.models.mongo import MongoDB
# from src.models.redshift import RedshiftDB

@celery.task(soft_time_limit=120)
def process_report(report):
    """Process report
    :param report: report id
    """
    try:
        print(f"process_report :: initing report {report} ...")
        print(f"process_report :: retrieving data...")
        # connect to DBs
        # redshift_instance = RedshiftDB()
        # mongodb_instance = MongoDB()        
        time.sleep(3)
        print(f"process_report :: building report {report} ...")
        # processing data...
        time.sleep(5)
        print(f"process_report :: saving report {report} ...")
        # save_report(spreedsheet, filename)
        time.sleep(3)
        print(f"process_report :: report {report} done!")

    except ReportWarningEmpty:
        logging.exception(f"process_report :: exception ReportWarningEmpty to report: {report}")
        message="There are no data to the input params"
        # redshift_instance.update_status_report_warning(report,message)
    except MongoError:
        logging.exception(f"process_report :: MongoError to report: {report}")
        message="Problems in connecting to MongoDB"
        # redshift_instance.update_status_report_warning(report,message)
    except RedshiftError:
        logging.exception(f"process_report :: exception RedshiftError to report: {report}")
        message="Problems in get data from Redshift"
        # redshift_instance.update_status_report_error(report,message)
    except SoftTimeLimitExceeded:        
        logging.exception(f"process_report :: exception SoftTimeLimitExceeded to report: {report}")
        message="Proccess time limit exceeded"
        # redshift_instance.update_status_report_error(report,message)
    except Exception as err:
        logging.exception(f"process_report :: something wrong happened to report: {report}: {err!r}")
        message="Something wrong happened in the processing"
        # redshift_instance.update_status_report_error(report,message)

    else:
        logging.info(f"process_report :: Report done to {report}")

    # mongodb_instance.close_connection()
    # redshift_instance.close_connection()


def create_report(data: dict) -> Tuple[bool, str]:
    try:
        ok = True
        uuid_key = ''
        start_dt,end_dt = None,None

        if not type(data['tenant_id'])==str:
            raise
        uuid_key+=data['tenant_id']

        if data['start_date']:
            # check dateformat
            date.fromisoformat(data['start_date'])
            uuid_key+=data['start_date']
            start_dt = datetime.strptime(data['start_date'], '%Y-%m-%d')
            start_dt = start_dt.replace(hour=0,minute=0,second=0)
        if data['end_date']:
            # check dateformat
            date.fromisoformat(data['end_date'])
            uuid_key+=data['end_date']
            end_dt = datetime.strptime(data['end_date'], '%Y-%m-%d')
            end_dt = end_dt.replace(hour=23,minute=59,second=59)

        request_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        uuid_key+=request_dt
        report = str(uuid.uuid5(uuid.NAMESPACE_DNS, uuid_key))
        res = {"report_id":report}

        process_report.apply_async(args=[report], task_id=report)

    except Exception as err:
        ok = False
        res = {'message': 'Bad input. Check if your input is correct.'}
        logging.info(f"create_report :: Bad input. Check if your input is correct. {err!r}")
    
    return ok, json.dumps(res)