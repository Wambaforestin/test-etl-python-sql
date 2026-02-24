import polars as pl
import utils

@utils.chronometre
def charge_joconde_json(path: str) -> pl.DataFrame:
    return pl.read_json(path, infer_schema_length=10000, schema_overrides={"references_merimee": pl.Utf8,})

fichier_json = "data/base-joconde-extrait.json"
df_pl = charge_joconde_json(fichier_json)
