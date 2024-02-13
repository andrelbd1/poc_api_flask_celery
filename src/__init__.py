import os
from flask import Flask, url_for
from flask_restx import Api
from flask_restx.apidoc import apidoc
from werkzeug.middleware.proxy_fix import ProxyFix
from celery import Celery, VERSION
from src.config import settings

celery = Celery(__name__, backend=settings.CELERY_BROKER_URL, broker=settings.CELERY_BROKER_URL)
if VERSION.major >= 5 and VERSION.minor >= 3: celery.conf.update(broker_connection_retry_on_startup=True)

def create_app():
    flask_app = Flask(__name__)
    flask_app.wsgi_app = ProxyFix(flask_app.wsgi_app, x_proto = 1, x_host = 1)
    
    flask_app.config['CELERY_BROKER_URL'] = settings.CELERY_BROKER_URL
    # flask_app.config['CELERY_RESULT_BACKEND'] = settings.CELERY_RESULT_BACKEND
    
    api = Api(
          version = "1.0.0", 
		      title = "Report Service", 
		      description = "Service to report.",
          doc='/'
    )

    from src.apis import api as pr

    api.add_namespace(pr)

    api.init_app(flask_app)
    return flask_app
