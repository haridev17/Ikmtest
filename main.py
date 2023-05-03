import datetime
import logging
import pyodbc
import psycopg2

postgres_connector = psycopg2.connect(
    host="localhost",
    database="Ecom",
    user="postgres",
    password="postgresadmin")

logging.basicConfig(level=logging.INFO)
mssql_connector = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                 "Server=.;"
                                 "Database=Ecom;"
                                 "Trusted_Connection=yes;"
                                 )

def migration_customers():

    # Execute a query
    cursor_sql_customers = mssql_connector.cursor()
    cursor_postgres_customers = postgres_connector.cursor()
    logging.info("Customers table migration initiated")
    cursor_sql_customers.execute('SELECT * FROM customers')
    customer_results = cursor_sql_customers.fetchall()
    count = 0
    for row in customer_results:
        count+=1
        name = row[1] if row[1]!=None else ""
        email = row[2] if row[2]!=None else ""
        cursor_postgres_customers.execute(f"INSERT INTO customers (id,name,email,phone)  VALUES ('{row[0]}','{name}','{email}','{row[3]}')",)
        postgres_connector.commit()
    cursor_sql_customers.close()
    cursor_postgres_customers.close()
    logging.info(f"Customer table {count} rows migrated")


def migration_orders():
    cursor_sql_orders = mssql_connector.cursor()
    cursor_sql_orders.execute('SELECT * FROM orders')
    order_results = cursor_sql_orders.fetchall()
    cursor_postgres_orders = postgres_connector.cursor()
    logging.info("Order table migration initiated")
    count =0
    for row in order_results:
        count+=1
        total_price = row[2] if row[2]!=None else 0
        created_at = row[3] if row[3]!=None else datetime.datetime.now()
        cursor_postgres_orders.execute(
            f"INSERT INTO orders (id,customer_id,total_price,created_at)  VALUES ('{row[0]}','{row[1]}','{total_price}','{created_at}')", )
        postgres_connector.commit()
    cursor_sql_orders.close()
    cursor_postgres_orders.close()
    logging.info(f"Order table {count} rows migrated")


def migration_products():
    # Fetch data from sql
    cursor_sql_products = mssql_connector.cursor()
    cursor_postgres_products = postgres_connector.cursor()
    logging.info("Product table migration initiated")
    cursor_sql_products.execute('SELECT * FROM products')

    products_results = cursor_sql_products.fetchall()
    count = 0

    for row in products_results:
        count+=1
        products = row[1] if row[1] != None else ""
        price = row[2] if row[2] != None else 0
        cursor_postgres_products.execute(
            f"INSERT INTO products (id,name,price)  VALUES ('{row[0]}','{products}','{price}')", )
        postgres_connector.commit()
    cursor_sql_products.close()
    cursor_postgres_products.close()
    logging.info(f"Product table {count} rows migrated")




if __name__ == '__main__':
    migration_customers()
    migration_orders()
    migration_products()


