import psycopg2
import getpass
import pandas as pd
import os
import re

# Load and merge data
df = pd.read_csv('data/v_commune_2024.csv')
garder = df[["COM", "REG", "DEP","LIBELLE"]]
dfP = pd.read_csv('data/serie_historique_data.CSV', delimiter=';', low_memory=False)
fusion = pd.merge(garder, dfP, left_on='COM', right_on='CODGEO', how='inner')
fusion = fusion.drop('COM', axis=1)
fusion = fusion.reset_index(drop=True)
print(fusion.head())
cache_id_codgeo = {}
cache_valid_com = {}
cache_id_codwedding = {}


def insert_regions(cursor, data):
    regions = data[['REG', 'LIBELLE']].drop_duplicates()
    with open('regions.csv', 'w') as f:
        for _, row in regions.iterrows():
            if not pd.isna(row['REG']):  # Skip over NaN values
                f.write(f"{int(row['REG'])},{row['LIBELLE']}\n")
    with open('regions.csv', 'r') as f:
        cursor.copy_expert("COPY region (id_region, nom_region) FROM STDIN WITH (FORMAT CSV)", f)
    os.remove('regions.csv')

def insert_departments(cursor, data):
    departments = data[['DEP', 'REG', 'LIBELLE']].drop_duplicates()
    with open('departments.csv', 'w') as f:
        for _, row in departments.iterrows():
            if not pd.isna(row['DEP']):  # Skip over NaN values
                f.write(f"{int(row['DEP'], 16)},{int(row['REG'])},{row['LIBELLE']}\n")
    with open('departments.csv', 'r') as f:
        cursor.copy_expert("COPY departement (id_departement, id_region, nom_departement) FROM STDIN WITH (FORMAT CSV)", f)
    os.remove('departments.csv')

def insert_communes(cursor, data):
    communes = data[['COM', 'DEP', 'LIBELLE']].drop_duplicates()  # Include LIBELLE column
    with open('communes.csv', 'w') as f:
        for _, row in communes.iterrows():
            if not pd.isna(row['DEP']):  # Skip over NaN values
                f.write(f"{int(row['COM'], 16)},{int(row['DEP'], 16)},{row['LIBELLE']}\n")  # Convert DEP from hexadecimal to decimal
                cache_valid_com[int(row['COM'], 16)] = True
    with open('communes.csv', 'r') as f:
        cursor.copy_expert("COPY commune (id_commune, id_departement, nom_commune) FROM STDIN WITH (FORMAT CSV)", f)
    os.remove('communes.csv')

def insert_lieux(cursor):
    insert_regions(cursor, pd.read_csv("data/v_region_2023.csv", delimiter=','))
    insert_departments(cursor, pd.read_csv("data/v_departement_2023.csv", delimiter=','))
    insert_communes(cursor, pd.read_csv("data/v_commune_2024.csv", delimiter=','))

# Function to insert populaton data
def insert_labels_pop(cursor, labels):
    cursor.execute("SELECT COALESCE(MAX(id_libelle), 0) FROM Libelle_stat_pop")
    max_id = cursor.fetchone()[0]
    labelList = labels[['COD_VAR', 'LIB_VAR']].drop_duplicates()
    with open('labels.csv', 'w') as f:
        for _, row in labelList.iterrows():
            if (row['COD_VAR'] != 'CODGEO'):
                max_id += 1
                f.write(f"{max_id},{row['COD_VAR']},{row['LIB_VAR']}\n")
    with open('labels.csv', 'r') as f:
        cursor.copy_expert("COPY libelle_stat_pop (id_libelle, code_stat, libelle_stat) FROM STDIN WITH (FORMAT CSV)", f)
    os.remove('labels.csv')

def insert_stat_codes(cursor, data):
    cursor.execute("SELECT COALESCE(MAX(id_stat_pop), 0) FROM stat_pop")
    max_id = cursor.fetchone()[0]
    with open('codes.csv', 'w') as f:
        for _, col in data.items():
            if col.name != 'CODGEO':
                max_id += 1
                year_start, year_end = date_from_code(col.name)
                if (year_start is None and year_end is None):
                    f.write(f"{max_id},{col.name},,\n")
                elif (year_end is None):
                    f.write(f"{max_id},{col.name},{year_start},\n")
                else:
                    f.write(f"{max_id},{col.name},{year_start},{year_end}\n")
    with open('codes.csv', 'r') as f:
        cursor.copy_expert("COPY stat_pop (id_stat_pop, code_stat, annee, annee_fin) FROM STDIN WITH (FORMAT CSV)", f)
    os.remove('codes.csv')

def insert_donnee_pop(cursor, data):
    with open('donnees_pop.csv', 'w') as f:
        for _, row in data.iterrows():
            id_commune = int(row['CODGEO'], 16)
            for col_name, value in row.items():
                if col_name != 'CODGEO' and is_valid(id_commune):
                    id_stat_pop = col_name
                    f.write(f"{id_commune},{get_id_for_codgeo(cursor,id_stat_pop)},{value}\n")
    with open('donnees_pop.csv', 'r') as f:
        cursor.copy_expert("COPY donnee_pop (id_commune, id_stat_pop, valeur) FROM STDIN WITH (FORMAT CSV)", f)
    os.remove('donnees_pop.csv')

def insert_data_pop(cursor, data):
    insert_stat_codes(cursor, data)
    insert_donnee_pop(cursor, data)

def get_id_for_codgeo(cursor, codgeo):
    if (cache_id_codgeo == {}):
        cursor.execute("SELECT id_stat_pop, code_stat FROM stat_pop")
        rows = cursor.fetchall()
        for row in rows:
            cache_id_codgeo[row[1]] = row[0]
    return cache_id_codgeo[codgeo]


# Function to insert wedding data
def insert_labels_weddings(cursor, labels):
    cursor.execute("SELECT COALESCE(MAX(id_stat_mariage), 0) FROM Libelle_stat_mariage")
    max_id = cursor.fetchone()[0]
    labelList = labels[['COD_VAR', 'LIB_VAR']].drop_duplicates()
    with open('labels.csv', 'w') as f:
        for _, row in labelList.iterrows():
            max_id += 1
            f.write(f"{max_id};{row['COD_VAR']};{row['LIB_VAR']}\n")
    with open('labels.csv', 'r') as f:
        cursor.copy_expert("COPY libelle_stat_mariage (id_stat_mariage, code_stat, libelle_stat) FROM STDIN WITH (FORMAT CSV, DELIMITER ';')", f)
    os.remove('labels.csv')

def insert_echantillon_mariage(cursor, data):
    cursor.execute("SELECT COALESCE(MAX(id_echantillon), 0) FROM echantillon_mariage")
    max_id = cursor.fetchone()[0]
    with open('echantillon.csv', 'w') as f:
        for _, row in data.iterrows():
            max_id += 1
            f.write(f"{max_id};{row['COD_MOD']};{row['LIB_MOD1']};{get_id_for_codwedding(cursor, row['COD_VAR'])}\n")
    with open('echantillon.csv', 'r') as f:
        cursor.copy_expert("COPY echantillon_mariage (id_echantillon, code_echantillon, libelle_echantillon, id_stat_mariage) FROM STDIN WITH (FORMAT CSV, DELIMITER ';')", f)
    os.remove('echantillon.csv')

def insert_donnee_weddings(cursor, data):
    dict = dictionnary_wedding_sample(cursor)
    with open('donnees_mariage.csv', 'w') as f:
        for _, row in data.iterrows():
            if (row[1].isdigit()):
                if (len(row[1]) == 4):
                    id_dep = int(row[1][2:4], 16)
                else :
                    id_dep = int(row[1], 16)
                id_sample = dict.get((row[2], get_id_for_codwedding(cursor, data.columns[2])), None)
                f.write(f"{id_dep},{id_sample},{row[3]},{row[0]},2021\n")
    with open('donnees_mariage.csv', 'r') as f:
        cursor.copy_expert("COPY donnee_mariage (id_departement, id_echantillon, valeur, type_couple, annee) FROM STDIN WITH (FORMAT CSV)", f)
    os.remove('donnees_mariage.csv')

def get_id_for_codwedding(cursor, codwedding):
    if (cache_id_codwedding == {}):
        cursor.execute("SELECT id_stat_mariage, code_stat FROM libelle_stat_mariage")
        rows = cursor.fetchall()
        for row in rows:
            cache_id_codwedding[row[1]] = row[0]
    return cache_id_codwedding[codwedding]

def dictionnary_wedding_sample(cursor):
    cursor.execute("SELECT id_echantillon, code_echantillon, id_stat_mariage FROM echantillon_mariage")
    rows = cursor.fetchall()
    dict = {}
    for row in rows:
        dict[(row[1], row[2])] = row[0]
    return dict

def insert_meta_weddings(cursor, labels):
    insert_labels_weddings(cursor, labels)
    insert_echantillon_mariage(cursor, labels)


# Utilitary methods
def is_valid(id_commune):
    return id_commune in cache_valid_com

def date_from_code(code):
    year = ''.join(re.findall(r'\d+', code))
    if (len(year) == 2):
        year = complete_year(year)
        return (year, None)
    if (len(year) == 4):
        return (complete_year(year[:2]), complete_year(year[2:4]))
    return (None, None)

def complete_year(year):
    if (int(year) <= 50):
        return '20' + year
    else:
        return '19' + year

def dep_reg_from_code(code):
    return (code[2:4], code[:2])

# Load the data
def load_labels_pop():
    df = pd.read_csv("data/meta_base-cc-serie-historique-2020.CSV", delimiter=';', usecols=['COD_VAR', 'LIB_VAR'])
    labels = df[['COD_VAR', 'LIB_VAR']]
    return labels

def load_data_historique():
    df = pd.read_csv("data/serie_historique_data.CSV", delimiter=';', low_memory=False)
    return df

def load_labels_weddings():
    df = pd.read_csv("data/metadonnees_irsocmar2021_annuels.csv", delimiter=';')
    return df

def load_data_weddings(file_num):
    if (file_num == 1 or file_num == 4 or file_num == 5 or file_num == 6):
        df = pd.read_csv("data/Dep" + str(file_num) + ".csv", delimiter=';')
        return df
    else:
        print("Invalid file number.")
        return None

# Main function to handle database connection and insertion
def main():
    USERNAME = input("Enter your username: ")
    PASS = getpass.getpass("Enter your password: ")

    try:
        conn = psycopg2.connect(f"host=pgsql dbname={USERNAME} user={USERNAME} password={PASS}")
        conn.autocommit = True
        cur = conn.cursor()
        labels_pop = load_labels_pop()
        data_historique = load_data_historique()
        labels_wed = load_labels_weddings()
        insert_lieux(cur)
        insert_labels_pop(cur, labels_pop)
        insert_meta_weddings(cur, labels_wed)

        insertion_type = input("What do you need to insert :\n1 - Population data\n2 - Weddings data\n3 - Everything\n")
        if (insertion_type == '1'):
            insert_data_pop(cur, data_historique)
        elif (insertion_type == '2'):
            wedding_file_number = input("Enter the number of the file containing your data or 0 to insert everything : ")
            if (wedding_file_number == '0'):
                data_wed = load_data_weddings(1)
                insert_donnee_weddings(cur, data_wed)
                data_wed = load_data_weddings(4)
                insert_donnee_weddings(cur, data_wed)
                data_wed = load_data_weddings(5)
                insert_donnee_weddings(cur, data_wed)
                data_wed = load_data_weddings(6)
                insert_donnee_weddings(cur, data_wed)
            else:
                data_wed = load_data_weddings(int(wedding_file_number))
                insert_donnee_weddings(cur, data_wed)
        elif (insertion_type == '3'):
            insert_data_pop(cur, data_historique)
            data_wed = load_data_weddings(1)
            insert_donnee_weddings(cur, data_wed)
            data_wed = load_data_weddings(4)
            insert_donnee_weddings(cur, data_wed)
            data_wed = load_data_weddings(5)
            insert_donnee_weddings(cur, data_wed)
            data_wed = load_data_weddings(6)
            insert_donnee_weddings(cur, data_wed)
        print("All data inserted successfully.")

    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
