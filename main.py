from src import create_app
from src.config import settings

app = create_app()

if __name__ == "__main__":
    debug=False
    if settings.ENV == "local":
        debug=True
        
    app.run(host='0.0.0.0',port='8000',debug=debug)