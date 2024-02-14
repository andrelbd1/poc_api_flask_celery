import os
from dotenv import load_dotenv

class Settings:
    
    load_dotenv(override=True)
    
    # Setting Environment
    ENV = os.getenv('ENV','local')
    
    # Mongo Credentials
    PREFIX_DB_MONGO = os.getenv('PREFIX_DB_MONGO','mongodb+srv')
    USER_DB_MONGO = os.environ["USER_DB_MONGO"]
    PW_DB_MONGO = os.environ["PW_DB_MONGO"]
    HOST_DB_MONGO = os.environ["HOST_DB_MONGO"]
    COMPLEMENT_DB_MONGO = os.getenv('COMPLEMENT_DB_MONGO','?retryWrites=true&w=majority')
    
    if PREFIX_DB_MONGO and PW_DB_MONGO and USER_DB_MONGO and HOST_DB_MONGO and COMPLEMENT_DB_MONGO:
        DB_MONGO_URI = f"{PREFIX_DB_MONGO}://{USER_DB_MONGO}:{PW_DB_MONGO}@{HOST_DB_MONGO}/{COMPLEMENT_DB_MONGO}"

    # Redshift Credentials
    HOST_DB_REDSHIFT = os.environ["HOST_DB_REDSHIFT"]
    DATABASE_DB_REDSHIFT = os.environ["DATABASE_DB_REDSHIFT"]
    PORT_DB_REDSHIFT = os.getenv('PORT_DB_REDSHIFT','5439')
    USER_DB_REDSHIFT = os.environ["USER_DB_REDSHIFT"]
    PW_DB_REDSHIFT = os.environ["PW_DB_REDSHIFT"]

    # Broker URL    
    CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL','redis://localhost:6379/0')
    # CELERY_RESULT_BACKEND = os.environ['CELERY_RESULT_BACKEND']

    # AWS Credentials 
    S3_ACCESS_KEY = os.environ['S3_ACCESS_KEY']
    S3_SECRET_KEY = os.environ['S3_SECRET_KEY']
    S3_BUCKET = os.environ['S3_BUCKET']
    
    S3_FILE_PATH = os.environ['S3_FILE_PATH']
        
settings = Settings()
