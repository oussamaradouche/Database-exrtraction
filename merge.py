import pandas as pd

# Load the commune data
df = pd.read_csv('data/v_commune_2024.csv')

# Selecting multiple columns - use a list for multiple columns
garder = df[["COM", "REG","DEP"]]

# Load the series historique data
dfP = pd.read_csv('data/serie_historique_data.CSV', delimiter=';', low_memory=False)


# Fusionner les ensembles de données "election" et "geo" sur les colonnes de code de commune
# Ensure that the column names used here match exactly those in your CSV files
fusion = pd.merge(garder, dfP, left_on='COM', right_on='CODGEO', how='inner')

# You mentioned merging with a "revenu" dataset on commune code but did not provide that part of the code.
# Assuming you have a DataFrame named `revenu` with a column 'CODGEO' you could add:
# fusion = pd.merge(fusion, revenu, on='CODGEO', how='inner')

# Show the first few rows of the original DataFrame and the merged DataFrame
fusion = fusion.drop('COM', axis=1)

print(fusion.head())
