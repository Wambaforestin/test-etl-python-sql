import polars as pl
import os

ficher_json = "data/base-joconde-extrait.json"
fichier_parquet = "data/base-joconde-extrait.parquet"

df = pl.read_json(ficher_json, infer_schema_length=10000, schema_overrides={"references_merimee": pl.Utf8})
df.write_parquet(fichier_parquet)

df_parquet = pl.read_parquet(fichier_parquet)
print(df_parquet.sample(5))  # Afficher un échantillon de 5 lignes du DataFrame pour vérifier que les données ont été chargées correctement
# Le stockage parquet stocke les données de manière plus compacte que le format JSON, les données sont organisées en colonnes et compressées, ce qui réduit la taille du fichier. En revanche, le format JSON stocke les données de manière plus verbeuse, 
# avec des balises et des structures de données plus complexes, ce qui peut entraîner une taille de fichier plus importante. Par conséquent, il est courant que les fichiers Parquet soient plus petits que les fichiers JSON contenant les mêmes données.
size_json = os.path.getsize(ficher_json) / (1024 ** 2)  # en mégaoctets
size_parquet = os.path.getsize(fichier_parquet) / (1024 ** 2)  # en mégaoctets
print(f"Taille du fichier JSON : {size_json:.2f} Mo")
print(f"Taille du fichier Parquet : {size_parquet:.2f} Mo")