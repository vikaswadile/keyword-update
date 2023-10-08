import psycopg2

# Database connection parameters for the source database (postgres)
db_params_source = {
    "host": "localhost",
    "database": "postgres",  # Change to the source database name
    "user": "postgres",      # Change to the source database user
    "password": "sql",       # Change to the source database password
}

# Database connection parameters for the target database
db_params_target = {
    "host": "localhost",
    "database": "coco-testing",  # Change to the target database name
    "user": "postgres",         # Change to the target database user
    "password": "sql",          # Change to the target database password
}

try:
    # Establish a connection to the source database (postgres) to fetch shop details
    conn_source = psycopg2.connect(**db_params_source)
    cursor_source = conn_source.cursor()

    # Get the value to add from the user
    new_keyword = input("Enter the new keyword value to add: ")

    # Get a comma-separated list of shop_ids (location IDs) from the user
    shop_ids_input = input("Enter the shop_ids (comma-separated) to update location_keyword for: ")
    shop_ids = [int(shop_id.strip()) for shop_id in shop_ids_input.split(",")]

    # Establish a connection to the target database
    conn_target = psycopg2.connect(**db_params_target)
    cursor_target = conn_target.cursor()

    for shop_id in shop_ids:
        # Fetch shop details for the shop_id
        cursor_source.execute("SELECT shop_id, location_id, shop_name FROM shop WHERE shop_id = %s;", (shop_id,))
        shop_info = cursor_source.fetchone()

        if shop_info:
            shop_id, location_id, shop_name = shop_info

            # Check if location_id is present in the shop_mapper table
            cursor_target.execute("SELECT id, location_id, location_keyword, location_name FROM shop_mapper WHERE location_id = %s;", (location_id,))
            records = cursor_target.fetchall()

            if records:
                print(f"Shop Information:")
                print(f"Shop ID: {shop_id}")
                print(f"Location ID: {location_id}")
                print(f"Shop Name: {shop_name}")
                print("Records in shop_mapper table for this location_id (Before Update):")
                for record in records:
                    print(f"ID: {record[0]}, Location ID: {record[1]}, Location Keyword: {record[2]}, Location Name: {record[3]}")
                print()

                # Prepare the new keyword as an array
                new_keyword_array = [new_keyword]

                # SQL query to update the location_keyword with a new value within the curly braces
                sql_query = """
                UPDATE shop_mapper
                SET location_keyword = location_keyword || %s
                WHERE location_id = %s;
                """

                # Execute the SQL query with parameters
                cursor_target.execute(sql_query, (new_keyword_array, location_id))
                conn_target.commit()
                print(f"Keyword value updated successfully for Location ID: {location_id}")
                
                # Print the updated records
                cursor_target.execute("SELECT id, location_id, location_keyword, location_name FROM shop_mapper WHERE location_id = %s;", (location_id,))
                updated_records = cursor_target.fetchall()
                print("Updated Records in shop_mapper table for this location_id:")
                for record in updated_records:
                    print(f"ID: {record[0]}, Location ID: {record[1]}, Location Keyword: {record[2]}, Location Name: {record[3]}")
                print()

            else:
                print(f"No records found in shop_mapper table for Location ID: {location_id}")
                print()
        else:
            print(f"Shop_id {shop_id} not found in the source database.")

except psycopg2.Error as e:
    print("Error:", e)

finally:
    if conn_source:
        cursor_source.close()
        conn_source.close()
    if conn_target:
        cursor_target.close()
        conn_target.close()

