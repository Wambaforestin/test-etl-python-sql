import polars as pl
import utils
from sqlalchemy import MetaData, Table, Column, String, Text, create_engine, insert, delete
from dotenv import load_dotenv
import urllib.parse
import os
import logging
import yaml
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Charger les variables d'environnement à partir du fichier .env
load_dotenv()

parameters = urllib.parse.quote_plus(
    # Pour vérifier si le driver ODBC est installé, j'ai tapé la cmd suivante (PowerShell) :
    # Get-OdbcDriver | Where-Object { $_.Name -like "*SQL Server*" }
    # Ensuite, j'ai utilisé le nom du driver trouvé dans la cmd ci-dessus pour construire la chaîne de connexion.
    
    f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv('SQLSERVER_HOST')};DATABASE={os.getenv('DATABASE_NAME')};UID={os.getenv('USER_SQLSERVER')};PWD={os.getenv('SQLSERVER_PASSWORD')};TrustServerCertificate=yes;"
)

@utils.chronometre_logging
def charger_fichier(path):
    return pl.read_json(path, infer_schema_length=10000, schema_overrides={"references_merimee": pl.Utf8})

fichier = 'data/base-joconde-extrait.json'
df = charger_fichier(fichier)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={parameters}",
    connect_args={"fast_executemany": True}
)

# utilisation de SQLAlchemy, le mode core pour charger les métadonnées de la table.
metadata = MetaData()

joconde_table = Table("joconde", metadata,
    Column("reference", String),
    Column("appellation", String),
    Column("auteur", String),
    Column("date_creation", String),
    Column("denomination", String),
    Column("region", String),
    Column("departement", String),
    Column("ville", String),
    Column("description", Text),
)
metadata.create_all(engine)

records = df.to_dicts() # Convertir le DataFrame Polars en une liste de dictionnaires.

BATCH_SIZE = config["destination"].get("batch_size", 1000)  # Valeur par défaut 1000 si non défini

with engine.begin() as connexion:
    # Vider la table avant d'insérer les nouvelles données
    connexion.execute(delete(joconde_table))
    # Insérer les données par batch
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        utils.chronometre_logging_lambda(
            f"Import batch {i} à {i + len(batch)}",
            lambda: connexion.execute(insert(joconde_table), batch)
        )
        logging.info(f"Batch inséré: {i} à {i + len(batch)}")

logging.info("Données importées")
