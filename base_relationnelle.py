from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import urllib.parse
import os
import sys
import pandas as pd

import logging

# Configure logging
logging.basicConfig(
    filename="etl_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Charger les variables d'environnement à partir du fichier .env
load_dotenv()

parameters = urllib.parse.quote_plus(
    # Pour vérifier si le driver ODBC est installé, j'ai tapé la cmd suivante (PowerShell) :
    # Get-OdbcDriver | Where-Object { $_.Name -like "*SQL Server*" }
    # Ensuite, j'ai utilisé le nom du driver trouvé dans la cmd ci-dessus pour construire la chaîne de connexion.
    
    f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.getenv('SQLSERVER_HOST')};DATABASE={os.getenv('DATABASE_NAME')};UID={os.getenv('USER_SQLSERVER')};PWD={os.getenv('SQLSERVER_PASSWORD')};TrustServerCertificate=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={parameters}")

query = """
SELECT TOP 30 *
FROM [dbo].[Customers];
"""

# Exécuter la requête et charger les résultats dans un DataFrame pandas
try:
    df = pd.read_sql(query, engine)
    logging.info("Requete executee avec succes.")
except SQLAlchemyError as e:
    logging.error(f"Erreur lors de l'exécution de la requête: {e}")
    sys.exit(1)
