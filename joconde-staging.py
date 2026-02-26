import polars as pl
import outils
from sqlalchemy import MetaData, create_engine, insert, text
from dotenv import load_dotenv
import urllib, logging, os
import yaml

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

logging.basicConfig(
    filename="staging.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

parameters = urllib.parse.quote_plus(
    # Pour vérifier si le driver ODBC est installé, j'ai tapé la cmd suivante (PowerShell) :
    # Get-OdbcDriver | Where-Object { $_.Name -like "*SQL Server*" }
    # Ensuite, j'ai utilisé le nom du driver trouvé dans la cmd ci-dessus pour construire la chaîne de connexion.
    
    f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv('SQLSERVER_HOST')};DATABASE={os.getenv('DATABASE_NAME')};UID={os.getenv('USER_SQLSERVER')};PWD={os.getenv('SQLSERVER_PASSWORD')};TrustServerCertificate=yes;"
)

fichier = 'data/joconde-base-extrait.json'
fichier_cache = "joconde_cache.feather"

if os.path.exists(fichier_cache):
    print("📦 Chargement depuis le cache feather...")
    df = pl.read_ipc(fichier_cache)
else:
    print("📥 Chargement depuis le JSON source...")
    df = pl.read_json(fichier, infer_schema_length=10000, schema_overrides={"references_merimee": pl.Utf8})

    # Sauvegarde rapide
    df.write_ipc(fichier_cache)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={parameters}", fast_executemany=True)

# ici je donne un champ de vision à SQLAlchemy pour trouver la table dans le schéma staging
metadata = MetaData(schema="staging")
# Par la suite, je vais utiliser la réflexion pour charger les métadonnées de la table à partir de la base de données, ce qui me permettra d'obtenir une référence à la table sans avoir à redéfinir sa structure dans le code. Cela rend le code plus flexible et moins sujet aux erreurs en cas de modifications futures de la structure de la table.
metadata.reflect(bind=engine)

joconde_table = metadata.tables[config["staging"]["table"]]

records = [
    {
        **row,
        # Ajouter des champs d'audit
        "source_system": config["audit"]["source_system"],
        "load_process": config["audit"]["load_process"]
    }
    for row in df.to_dicts() # Convertir le DataFrame Polars en une liste de dictionnaires.
]

with engine.begin() as conn:
    conn.execute(text(f"TRUNCATE TABLE {config['staging']['table']};"))
    outils.chronometre_logging_lambda("Import SQL Server", lambda: conn.execute(insert(joconde_table), records))

logging.info("Données importées")