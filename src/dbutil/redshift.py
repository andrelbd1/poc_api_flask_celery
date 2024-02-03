import os
import redshift_connector
from src.config import settings
from datetime import datetime
import json

class RedshiftDB:
    def __init__(self) -> None:
        try:
            redshift_connector.paramstyle = 'pyformat'
            self.conn = redshift_connector.connect(
                                host=settings.HOST_DB_REDSHIFT,
                                database=settings.DATABASE_DB_REDSHIFT,
                                port=settings.PORT_DB_REDSHIFT,
                                user=settings.USER_DB_REDSHIFT,
                                password=settings.PW_DB_REDSHIFT
                            )
            self.cursor = self.conn.cursor()
            self.status={
                'in_queue':'In Queue',
                'in_progress':'In Progress',
                'error':'Error',
                'warning':'Warning',
                'done':'Done',
                'not_requested':'Not Requested'
            }
        except Exception as err:
            logging.exception(f"{type(self).__name__} :: {err!r}")

    def close_connection(self):
        try:
            self.conn.close()
        except Exception as err:
            logging.exception(f"{type(self).__name__} :: {err!r}")

    def execute_query(self, query, values=None, return_data=None):
        try:
            self.cursor.execute(query, values)
            res=None
            if return_data == 'fetchone':
                res = self.cursor.fetchone()
            elif return_data == 'fetchall':
                res = self.cursor.fetchall()
            elif return_data == 'dataframe':
                res = self.cursor.fetch_dataframe()
            self.conn.commit()
            return res
        except Exception as err:
            logging.exception(f"{type(self).__name__} :: Failed with error: {err!r}")
            return None

    def insert_report(self, report_id: str, request_date: datetime, tenant_id: str,
                            start_date: datetime=None, end_date: datetime=None,
                            status: str='in_queue', location: str=None) -> None:
  
        query='insert into reports(report_id, status, tenant_id, start_date, end_date, location, request_date) '
        query+='VALUES(%(report_id)s, %(status)s, %(tenant_id)s, %(start_date)s, %(end_date)s, %(location)s, %(request_date)s)'
        
        values={}
        values.update({"report_id":report_id})
        values.update({"status":self.status['in_queue']})
        if status in self.status.keys():
            values.update({"status":self.status[status]})
        values.update({"tenant_id":tenant_id})
        if start_date and end_date:
            values.update({"start_date":start_date.strftime('%Y-%m-%d %H:%M:%S')})
            values.update({"end_date":end_date.strftime('%Y-%m-%d %H:%M:%S')})
        else:
            values.update({"start_date":start_date})
            values.update({"end_date":end_date})
        values.update({"location":location})
        values.update({"request_date":request_date.strftime('%Y-%m-%d %H:%M:%S')})

        self.execute_query(query, values)

    def update_status_report_progress(self, report_id: str) -> None:
        query='update reports set status = %(status)s where report_id = %(report_id)s'
        
        values={}
        values.update({"report_id":report_id})
        values.update({"status":self.status['in_progress']})

        self.execute_query(query, values)
    
    def update_status_report_done(self, report_id: str, location: str) -> None:
        query='update reports set status = %(status)s, location = %(location)s where report_id = %(report_id)s'
        
        values={}
        values.update({"report_id":report_id})
        values.update({"location":location})
        values.update({"status":self.status['done']})

        self.execute_query(query, values)

    def update_status_report_error(self, report_id: str, message: str) -> None:
        query='update reports set status = %(status)s, message = %(message)s where report_id = %(report_id)s'
        
        values={}
        values.update({"report_id":report_id})
        values.update({"status":self.status['error']})
        values.update({"message":message})

        self.execute_query(query, values)

    def get_request_date_in_progress_in_queue_tasks(self) -> list:
        query='select request_date from reports where status = %(in_queue)s or status = %(in_progress)s'

        values={}
        values.update({'in_queue':self.status['in_queue']})
        values.update({'in_progress':self.status['in_progress']})

        result = self.execute_query(query, values, 'dataframe')

        return result[result.columns[0]].tolist()

    def update_status_report_warning(self, report_id: str, message: str) -> None:        
        query='update reports set status = %(status)s, message = %(message)s where report_id = %(report_id)s'
        values={}
        values.update({"report_id":report_id})
        values.update({"status":self.status['warning']})
        values.update({"message":message})

        self.execute_query(query, values)
    
    def get_invoice_report(self, report_id: str) -> dict:
        query='select status, tenant_id, start_date, end_date, location, message, request_date from reports where report_id = %(report_id)s'
 
        values={}
        values.update({"report_id":report_id})
        
        result = self.execute_query(query, values, 'fetchone')

        res={}
        res.update({'status':self.status['not_requested']})
        if result:
            res.update({'status':result[0]})
            res.update({'tenant_id':result[1]})
            res.update({'start_date':result[2]})
            if result[2]:
                res.update({'start_date':result[2].strftime('%Y-%m-%d %H:%M:%S')})
            res.update({'end_date':result[2]})
            if result[3]:
                res.update({'end_date':result[3].strftime('%Y-%m-%d %H:%M:%S')})
            res.update({'location':result[4]})
            res.update({'message':result[5]})
            res.update({'request_date':result[7].strftime('%Y-%m-%d %H:%M:%S')})            

        return res
    
    def get_status_report(self, report_id: str) -> dict:
        query='select status, request_date, location, message from reports where report_id = %(report_id)s'

        values={}
        values.update({"report_id":report_id})

        result = self.execute_query(query, values, 'fetchone')

        res={}
        res.update({'status':self.status['not_requested']})
        if result:
            res.update({'status':result[0]})
            res.update({'request_date':result[1].strftime('%Y-%m-%d %H:%M:%S')})
            if result[2]:
                res.update({'location':result[2]})
            if result[3]:
                res.update({'message':result[3]})

        return res