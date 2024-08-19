import psycopg2
import getpass


def list_departments_in_region(cursor, region_id):
    cursor.execute("""
        SELECT d.id_departement, d.id_region
        FROM Departement d
        WHERE d.id_region = %s
    """, (region_id,))
    departments = cursor.fetchall()
    print(f"Departments in Region {region_id}:")
    for dep in departments:
        print(f"Department ID: {dep[0]} - Region ID: {dep[1]}")







# Main function to establish a database connection and execute queries
def main():
    USERNAME = input("Enter your username: ")
    PASS = getpass.getpass("Enter your password: ")

    try:
        conn = psycopg2.connect(f"host=pgsql dbname={USERNAME} user={USERNAME} password={PASS}")
        
        cursor = conn.cursor()

        # Execute the functions with example parameters
        list_departments_in_region(cursor, 11)
    


    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
