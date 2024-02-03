#!/usr/bin/env python
import os
import warnings
from src.apis import celery, create_app

warnings.filterwarnings('ignore')

app = create_app()
app.app_context().push()