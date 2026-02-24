from base_relationnelle import df

def main():
    print("Hello from test-etl-python-sql!")
    print(df.sample(10))  # Afficher un échantillon de 10 lignes du DataFrame pour vérifier que les données ont été chargées correctement


if __name__ == "__main__":
    main()
