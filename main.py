from base_relationnelle import df, pl_df

def main():
    
    print("Hello from test-etl-python-sql!")
    print(df.sample(10))  # Afficher un échantillon de 10 lignes du DataFrame pour vérifier que les données ont été chargées correctement
    
    # calcule la mémoire utilisée par les DataFrames pandas et Polars
    mem_pandas = df.memory_usage(deep=True).sum() / (1024 ** 2)  # en mégaoctets
    mem_polars = pl_df.estimated_size(unit="mb")  # en mégaoctets
    
    print(f"Le DataFrame pandas utilise environ {mem_pandas:.2f} Mo de mémoire.")
    print(f"Le DataFrame Polars utilise environ {mem_polars:.2f} Mo de mémoire.")

if __name__ == "__main__":
    main()
