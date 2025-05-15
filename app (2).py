import pandas as pd 
import numpy as np
import psycopg2 
from psycopg2 import sql



# Step 1. Loading the CSV in data file giving them a random variable 
# name to save the file
#actor_file = pd.read_csv("actor.csv")
address_file = pd.read_csv("address.csv")
category_file = pd.read_csv("category.csv")
#city_file = pd.read_csv("city.csv")
#country_file = pd.read_csv("country.csv")
customer_file = pd.read_csv("customer.csv")
film_file = pd.read_csv("film.csv")
file_actor_file = pd.read_csv("film_actor.csv")
film_category_file = pd.read_csv("film_category.csv")
inventory_file = pd.read_csv("inventory.csv")
#language_file = pd.read_csv("language.csv")
payment_file = pd.read_csv("payment.csv")
rental_file = pd.read_csv("rental.csv")
store_file = pd.read_csv("store.csv")
#staff_file = pd.read_csv("staff.csv")

#Step 2. Printing the first five rows of the data file to see the data

#print(category_file.head())
#print(customer_file.head())
print(film_file.head())


###3. Applying ETL for the files for the files we need, like transforming the data 

# For question 1, we need to find the total rental amount for each film_id, so first we will check our rental table 


#print(rental_file.dtypes)
#checking to see if there were any values present
# print(rental_file['rental_id'].isnull().sum()) # 0 null values
# print(rental_file['rental_date'].isnull().sum()) # 0 null values
# print(rental_file['inventory_id'].isnull().sum()) # 0 null values
# print(rental_file['customer_id'].isnull().sum()) # 0 null values
# print(rental_file['return_date'].isnull().sum()) # 183 null values
# print(rental_file['staff_id'].isnull().sum()) # 0 null values
# print(rental_file['last_update'].isnull().sum()) # 0 null values

# print(rental_file['rental_id'].isnull().sum()) # 0 null values
# print(rental_file['rental_date'].isnull().sum()) # 0 null values
# print(rental_file['inventory_id'].isnull().sum()) # 0 null values
# print(rental_file['customer_id'].isnull().sum()) # 0 null values
# print(rental_file['return_date'].isnull().sum()) # 183 null values
# print(rental_file['staff_id'].isnull().sum()) # 0 null values
# print(rental_file['last_update'].isnull().sum()) # 0 null values

#converting those columns which have  datetime format to load for postgresql
rental_file['rental_date'] = pd.to_datetime(rental_file['rental_date'])
rental_file['return_date'] = pd.to_datetime(rental_file['return_date'])
rental_file['last_update'] = pd.to_datetime(rental_file['last_update'])

###inventory table
inventory_file['last_update'] = pd.to_datetime(inventory_file['last_update'])

# Check for null values in the inventory_file DataFrame
#print(inventory_file['inventory_id'].isnull().sum())  # Check null values in inventory_id = 0
#print(inventory_file['film_id'].isnull().sum())       # Check null values in film_id = 0
#print(inventory_file['store_id'].isnull().sum())      # Check null values in store_id = 0
#print(inventory_file['last_update'].isnull().sum())   # Check null values in last_update = 0

####film 
film_file['last_update'] = pd.to_datetime(film_file['last_update'])

####category
category_file['last_update'] = pd.to_datetime(category_file['last_update'])

####customer
customer_file['last_update'] = pd.to_datetime(customer_file['last_update'])


###inventory 
inventory_file['last_update'] = pd.to_datetime(inventory_file['last_update'])

####filmcategory 
film_category_file['last_update'] = pd.to_datetime(film_category_file['last_update'])

#####payment
payment_file['payment_date'] = pd.to_datetime(payment_file['payment_date'])


try:
    conn = psycopg2.connect(database='film_rentals', user='postgres', password='sameer123', host='localhost', port='5432', options='-c client_encoding=UTF8')
    print("Connection successful")

except:
    print("Database Error in connecting") 

if conn:
    cur = conn.cursor()

    try: 
        drop_table_query = """
        DROP TABLE IF EXISTS dim_film CASCADE;
        DROP TABLE IF EXISTS dim_film_category CASCADE;
        DROP TABLE IF EXISTS dim_customer CASCADE;
        DROP TABLE IF EXISTS fact_rental CASCADE;
        DROP TABLE IF EXISTS payment CASCADE;
        DROP TABLE IF EXISTS rental CASCADE; 
        DROP TABLE IF EXISTS inventory CASCADE; 
        DROP TABLE IF EXISTS film_category CASCADE; 
        """
        cur.execute(drop_table_query)
        conn.commit()
    except Exception as e:
        print(f"Error dropping tables: {e}")

    create_table_query = """
    CREATE TABLE dim_film (
    film_id INT PRIMARY KEY,
    title VARCHAR(255),
    description TEXT,
    category_id INT,
    release_year INT,
    rental_duration INT,
    rental_rate DECIMAL,
    length INT,
    rating VARCHAR(10),
    special_features TEXT,
    last_update TIMESTAMP
    );
    """

    try:
        cur.execute(create_table_query)
        conn.commit()
        print("Table Film created successfully")
    except:
        print("Error creating table  or the table has already been created")

    create_table_query_2 = """
    
    CREATE TABLE dim_film_category (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(255),
    last_update TIMESTAMP
    );
    """
    try:
        cur.execute(create_table_query_2)
        conn.commit()
        print("Table Category created successfully")
    except:
        print("Error creating table  or the table has already been created")

    create_table_query_3 = """
    CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    address_id INT,
    active BOOL,
    create_date TIMESTAMP,
    last_update TIMESTAMP
    );
    """

    try:
        cur.execute(create_table_query_3)
        conn.commit()
        print("Table Customer created successfully")
    except:
        print("Error creating table  or the table has already been created")

    create_table_query_4 = """
    CREATE TABLE IF NOT EXISTS rental (
    rental_id INTEGER PRIMARY KEY NOT NULL, 
    rental_date TIMESTAMPTZ,
    inventory_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    return_date TIMESTAMPTZ,
    staff_id INTEGER NOT NULL, 
    last_update TIMESTAMPTZ
    );
    """
    try:
        cur.execute(create_table_query_4)
        conn.commit()
        print("Table Rental created successfully")
    except:
        print("Error creating table or the table has already been created")



    create_table_query_5 = """

    CREATE TABLE IF NOT EXISTS payment (
        payment_id INT PRIMARY KEY,
        customer_id INT,
        staff_id INT,
        rental_id INT,
        amount DECIMAL,
        payment_date TIMESTAMPTZ
            );
    """
    try:
        cur.execute(create_table_query_5)
        conn.commit()
        print("Table Payment created successfully")
    except:
        print("Error creating table or the table has already been created")


    create_table_query_6 = """
    CREATE TABLE fact_rental (
    rental_id INT,
    film_id INT,
    customer_id INT,
    payment_id INT,
    payment DECIMAL,
    rental_count INT,  -- Column to store total rental count for this transaction or film
    film_category_name VARCHAR(255),
    FOREIGN KEY (rental_id) REFERENCES rental (rental_id),
    FOREIGN KEY (film_id) REFERENCES dim_film(film_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (payment_id) REFERENCES payment(payment_id)
    );
    """
    try:
        cur.execute(create_table_query_6)
        conn.commit()
        print("Table Fact Rental created successfully")
    except Exception as e:
        print("Error creating table  or the table has already been created, {}".format(e))

    create_table_query_7 = """
        CREATE TABLE IF NOT EXISTS inventory (
    inventory_id INTEGER PRIMARY KEY NOT NULL,
    film_id INTEGER NOT NULL,
    store_id INTEGER NOT NULL,
    last_update TIMESTAMPTZ
    );"""
    try:
        cur.execute(create_table_query_7)
        conn.commit()
        print("Table Inventory created successfully")
    except Exception as e:
        print("Error creating table  or the table has already been created, {}".format(e))

    create_table_query_8 = """
    CREATE TABLE IF NOT EXISTS film_category (
    film_id INTEGER NOT NULL, 
    category_id INTEGER NOT NULL, 
    last_update TIMESTAMPTZ,
    FOREIGN KEY (film_id) REFERENCES dim_film (film_id)
    );
    """
    try:
        cur.execute(create_table_query_8)
        conn.commit()
        print("Table Category created successfully")
    except Exception as e:
        print("Error creating table  or the table has already been created, {}".format(e))




    try: 
        for _, row in film_file.iterrows():
            film_id = int(row['film_id'])
            title = row['title']
            description = row['description']
            release_year = int(row['release_year']) if pd.notnull(row['release_year']) else None
            rental_duration = int(row['rental_duration']) if pd.notnull(row['rental_duration']) else None
            rental_rate = float(row['rental_rate']) if pd.notnull(row['rental_rate']) else None
            length = int(row['length']) if pd.notnull(row['length']) else None
            length = int(row['length']) if pd.notnull(row['length']) else None
            rating = row['rating']
            last_update = row['last_update'].tz_localize(None) 
            special_features = row['special_features']
            fulltext = row['fulltext'] if 'fulltext' in row else None  # Check if 'fulltext' column exists
            
            try:
                insert_query = sql.SQL("INSERT INTO dim_film (film_id, title, description, release_year, rental_duration, rental_rate, length, rating, special_features, last_update) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")

                cur.execute(insert_query, (film_id, title, description, release_year, rental_duration, rental_rate, length, rating, special_features, last_update))
            except Exception as e:
                print(f"Error inserting film_date: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        print("Inserted film_date successfully")
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()

    try:
        for _, row in category_file.iterrows():
            category_id = int(row['category_id'])
            category_name = row['name']
            last_update = row['last_update'].tz_localize(None) 

            insert_query = sql.SQL("INSERT INTO dim_film_category (category_id, category_name, last_update) VALUES (%s, %s, %s)")
            cur.execute(insert_query, (category_id, category_name, last_update))

        conn.commit()
        print("Inserted category_date successfully")
    except Exception as e:
        print(f"Error inserting data: {e}")

    try:
        for _, row in customer_file.iterrows():
            customer_id = int(row['customer_id'])
            first_name = row['first_name']
            last_name = row['last_name']
            email = row['email']
            address_id = int(row['address_id']) if pd.notnull(row['address_id']) else None
            active = bool(row['active']) if pd.notnull(row['active']) else None
            create_date = row['create_date']
            last_update = row['last_update'].tz_localize(None) 

            try:
                insert_query = sql.SQL("INSERT INTO dim_customer (customer_id, first_name, last_name, email, address_id, active, create_date, last_update) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
                cur.execute(insert_query, (customer_id, first_name, last_name, email, address_id, active, create_date, last_update))
            except Exception as e:
                print(f"Error inserting customer data: {e}")
                conn.rollback()
                continue

        conn.commit()
        print("Inserted customer data successfully")

    except Exception as e:
        print(f"Error inserting customer data: {e}")
        conn.rollback()        


    try: 
        for _, row in payment_file.iterrows():
            payment_id = int(row['payment_id'])
            customer_id = int(row['customer_id'])
            staff_id = int(row['staff_id'])
            rental_id = int(row['rental_id'])
            amount = float(row['amount']) if pd.notnull(row['amount']) else None
            payment_date = row['payment_date'].tz_localize(None)

            try:
                insert_query = sql.SQL("INSERT INTO payment (payment_id, customer_id, staff_id, rental_id, amount, payment_date) VALUES (%s, %s, %s, %s, %s, %s)")
                cur.execute(insert_query, (payment_id, customer_id, staff_id, rental_id, amount, payment_date))
            except Exception as e:
                print(f"Error inserting payment data: {e}")
                conn.rollback()
                continue

        conn.commit()
        print("Inserted payment data successfully")
    except Exception as e:
        print(f"Error inserting payment data: {e}")
        conn.rollback()




    try: 
        for _, row in rental_file.iterrows():
            rental_id = int(row['rental_id'])
            rental_date = row['rental_date'].tz_localize(None)  # Remove any existing timezone if needed
            inventory_id = int(row['inventory_id'])
            customer_id = int(row['customer_id'])
            return_date = row['return_date'].tz_localize(None) if pd.notnull(row['return_date']) else None
            staff_id = int(row['staff_id'])
            last_update = row['last_update'].tz_localize(None)

            try:
                insert_query = sql.SQL("INSERT INTO rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update) VALUES (%s, %s, %s, %s, %s, %s, %s)")
                cur.execute(insert_query, (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update))
            except Exception as e:
                print(f"Error inserting rental data: {e}")
                conn.rollback()
                continue

        conn.commit()
        print("Inserted rental data successfully")
    except:
        print("Error inserting rental data")
        conn.rollback()

    try: 
        for _, row in inventory_file.iterrows():
            inventory_id = int(row['inventory_id'])
            film_id = int(row['film_id'])
            store_id = int(row['store_id'])
            last_update = row['last_update'].tz_localize(None)

            try:
                insert_query = sql.SQL("INSERT INTO Inventory (inventory_id, film_id, store_id, last_update) VALUES (%s, %s, %s, %s)")

                cur.execute(insert_query, (inventory_id, film_id, store_id, last_update))

            except Exception as e:
                print(f"Error inserting inventory_date: {e}")
                conn.rollback()
                continue
        conn.commit()
        print("Inserted inventory_date successfully")
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()    

    try: 
        for _, row, in film_category_file.iterrows():
            category_id = int(row['category_id'])
            film_id = int(row['film_id'])
            last_update = row['last_update'].tz_localize(None)

            try: 
                insert_query = sql.SQL("INSERT INTO film_category (film_id, category_id, last_update) VALUES (%s, %s, %s)")
                cur.execute(insert_query, (film_id, category_id,last_update ))
            except Exception as e: 
                print("Error Inserting category data")
                conn.rollback()
                continue
        conn.commit()
        print("Category data inserted successfully")
    except Exception as e: 
        print("Error inserting category data: {}".format(e))
        conn.rollback()
        


    try:
        insert_query_2 = """
        INSERT INTO fact_rental (rental_id, film_id, customer_id, payment_id, payment, rental_count, film_category_name)
        SELECT 
        r.rental_id,
        i.film_id,
        r.customer_id,
        p.payment_id,
        p.amount,
        COUNT(r.rental_id) OVER (PARTITION BY i.film_id) AS rental_count,
        fc.category_name AS "FILM Category"
        FROM rental r
        
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN dim_film f ON i.film_id = f.film_id
        JOIN film_category fc_map ON f.film_id = fc_map.film_id
        JOIN dim_film_category fc ON fc_map.category_id = fc.category_id
        JOIN dim_customer c ON r.customer_id = c.customer_id
        JOIN payment p ON r.rental_id = p.rental_id
        
        """
        try:
            conn.rollback()
            cur.execute(insert_query_2)
            conn.commit()
        #cur.execute(insert_query_2)
            print("Inserte Successfully")

        except Exception as e:
            print("Error: {}".format(e))
            conn.rollback()

    except Exception as e:
        print("ERROR: {}".format(e))

    
    
    



    # # finally:
    # #     cur.close()
    # #     conn.close()











