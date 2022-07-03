import logging
import os

import awsgi
import boto3
import dash_bootstrap_components as dbc
import mitzu.model as M
import mitzu.notebook.model_loader as ML
import mitzu.project as P
import mitzu.webapp.webapp as MWA
from dash import Dash, html

S3_BUCKET = os.getenv("MITZU_MODELS_BUCKET", None)
MODEL_PATH = os.getenv("MITZU_MODEL", None)

logging.getLogger().setLevel(logging.INFO)


def read_project_from_s3() -> bytes:
    logging.info(f"Reading model from {S3_BUCKET}/{MODEL_PATH}")
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(S3_BUCKET)
    obj = bucket.Object(MODEL_PATH)
    return obj.get()["Body"].read()


def create_app():
    logging.info(f"Creating Mitzu web app ------------------------------------------")
    app = Dash(
        __name__,
        compress=False,
        external_stylesheets=[
            dbc.themes.MATERIA,
            dbc.icons.BOOTSTRAP,
            "assets/layout.css",
            "assets/components.css",
        ],
    )

    project_binary = read_project_from_s3()
    logging.info(f"Loading project from file")
    deds = M.DiscoveredEventDataSource.load_from_project_binary(project_binary)
    logging.info(f"Creating notebook dataset model")
    dm = ML.ModelLoader().create_datasource_class_model(deds)
    logging.info(f"Initializing Mitzu web app for model")
    MWA.init_app(app, dm)
    logging.info(f"Finished initialization")
    return app


app = create_app()


def handler(event, context):
    return awsgi.response(app.server, event, context)
