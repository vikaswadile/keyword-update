import csv
import psycopg2
from psycopg2 import sql

# Database connection parameters
db_params = {
    'dbname': 'coco-testing',
    'user': 'postgres',
    'password': 'sql',
    'host': '',
    'port': '5432',
}

# CSV file path
csv_file_path = '/home/abc/keyword-file/test-keyword-1.csv'

# SQL statement for inserting data
insert_query = sql.SQL("""
    INSERT INTO shop_mapper (location_id, location_keyword, location_name, weather)
    VALUES (%s, %s, %s, %s)
""")

# Function to read CSV and insert data into PostgreSQL
def insert_data_from_csv():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Open and read data from the CSV file
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip the header row if it exists

            for row in csv_reader:
                # Convert row values to the appropriate data types
                location_id = int(row[0])  # Assuming location_id is an integer
                location_keyword = row[1].split(',')  # Assuming location_keyword is a comma-separated string
                location_name = row[2]
                weather = row[3]

                # Execute the insert query with data from the CSV
                cursor.execute(insert_query, (location_id, location_keyword, location_name, weather))

        # Commit the changes and close the connection
        conn.commit()
        print("Data inserted successfully!")

    except psycopg2.Error as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Call the function to insert data from CSV to PostgreSQL
insert_data_from_csv()

