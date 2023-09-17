import psycopg2

# Database connection parameters for the target database
db_params_target = {
    "host": "localhost",
    "database": "coco-testing",
    "user": "postgres",
    "password": "sql",
}

# Database connection parameters for the source database (postgres)
db_params_source = {
    "host": "localhost",
    "database": "postgres",  # Change to the source database name
    "user": "postgres",      # Change to the source database user
    "password": "sql",       # Change to the source database password
}

try:
    # Establish a connection to the source database (postgres) to fetch location_id
    conn_source = psycopg2.connect(**db_params_source)
    cursor_source = conn_source.cursor()

    # Establish a connection to the target database
    conn_target = psycopg2.connect(**db_params_target)
    cursor_target = conn_target.cursor()

    # Get the shop_ids from the user with input validation
    shop_ids_input = input("Enter the shop_ids (comma-separated) to fetch location_ids: ")
    shop_ids = [int(shop_id.strip()) for shop_id in shop_ids_input.split(",")]

    for shop_id in shop_ids:
        # Query the source database (postgres) to fetch location_id for the given shop_id
        cursor_source.execute("SELECT location_id FROM shop WHERE shop_id = %s;", (shop_id,))
        location_id_row = cursor_source.fetchone()

        if location_id_row:
            location_id = location_id_row[0]

            new_value = input(f"Enter the new value to add to location_keyword for shop_id {shop_id}: ")

            # Check if the location_id exists in the table of the target database
            cursor_target.execute("SELECT id FROM shop_mapper WHERE location_id = %s;", (location_id,))
            row = cursor_target.fetchone()

            if row:
                # Location_id exists, update the location_keyword array
                # Ensure that new_value is enclosed within curly braces to form an array
                new_value_array = "{" + new_value + "}"
                cursor_target.execute("UPDATE shop_mapper SET location_keyword = location_keyword || %s WHERE location_id = %s;", (new_value_array, location_id))
                conn_target.commit()
                print(f"New value added successfully for shop_id {shop_id}!")

                # Fetch and print the shop information for the updated shop_id
                cursor_source.execute("SELECT shop_id, location_id, shop_name FROM shop WHERE shop_id = %s;", (shop_id,))
                shop_info = cursor_source.fetchone()
                if shop_info:
                    print("Updated Shop Information:")
                    print(f"Shop ID: {shop_info[0]}")
                    print(f"Location ID: {shop_info[1]}")
                    print(f"Shop Name: {shop_info[2]}")
                else:
                    print("Shop information not found.")

            else:
                print(f"Location_id {location_id} not found in the table.")

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

