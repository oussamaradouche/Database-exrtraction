import psycopg2
import getpass

def list_departments_in_region(cursor, region_id):
    cursor.execute("""
        SELECT d.nom_departement, r.nom_region
        FROM Departement d
        JOIN Region r ON d.id_region = r.id_region
        WHERE d.id_region = %s
    """, (region_id,))
    departments = cursor.fetchall()
    print(f"Departments in Region {departments[0][1]}:")
    for dep in departments:
        print(f"- {dep[0]}")




def most_and_least_populated_regions(cursor):
    cursor.execute("""
        SELECT r.nom_region, SUM(dp.valeur) as total_population
        FROM Region r
        JOIN Departement d ON r.id_region = d.id_region
        JOIN Commune c ON d.id_departement = c.id_departement
        JOIN Donnee_pop dp ON c.id_commune = dp.id_commune
        JOIN Stat_pop sp ON dp.id_stat_pop = sp.id_stat_pop
        WHERE sp.code_stat = 'P20_POP'
        GROUP BY r.id_region
        ORDER BY total_population DESC
    """)
    results = cursor.fetchall()
    if results:
        print(f"Most populated region: Region {results[0][0]} with {int(results[0][1])} inhabitants.")
        print(f"Least populated region: Region {results[-1][0]} with {int(results[-1][1])} inhabitants.")

def nb_weddings_per_departement(cursor):
    cursor.execute("""
        SELECT d.id_departement, d.nom_departement, SUM(dm.valeur)
        FROM Donnee_mariage dm
        JOIN Departement d ON dm.id_departement = d.id_departement
        GROUP BY d.id_departement
        ORDER BY SUM(dm.valeur) DESC
    """)
    results = cursor.fetchall()
    print("Number of weddings per department:")
    for dep in results:
        print(f"- {dep[1]}: {dep[2]}")

#Main function to establish a database connection and execute queries
def main():
    USERNAME = input("Enter your username: ")
    PASS = getpass.getpass("Enter your password: ")

    try:
        conn = psycopg2.connect(f"host=pgsql dbname={USERNAME} user={USERNAME} password={PASS}")

        cursor = conn.cursor()

        while(True):
            # Execute the functions with example parameters
            question_type = input("Enter the question number (1, 2 or 3):\n1 - List departments in a region\n2 - Most and least populated regions\n3 - Number of weddings per department\n4 - Exit\n")

            if question_type == '1':
                region_id = input("\nEnter the region ID: ")
                list_departments_in_region(cursor, region_id)
            elif question_type == '2':
                most_and_least_populated_regions(cursor)
            elif question_type == '3':
                nb_weddings_per_departement(cursor)
            elif question_type == '4':
                break
            print("\n")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

main()