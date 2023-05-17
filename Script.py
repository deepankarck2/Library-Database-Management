import mysql.connector
import csv
from credentials import HOST, USER, PASSWORD, DATABASE

# Attr
"""
This class represents an attribute (or column) in a SQL table.
"""
class Attr:
    def __init__(self, name: str ="", type: str ="", constraints: str ="") -> None:
        self.name = name
        self.type = type
        self.constraints = constraints

# Foreign keys
"""
This class represents a foreign key in a SQL table.
"""
class Foreign:
    def __init__(self, name: str ="", table: str = "", refer:str ="",  constraints: str ="") -> None:
        self.name = name
        self.table = table
        self.refer = refer
        self.constraints = constraints

# Table class
"""
This class represents a SQL table.
"""
class Table:
    def __init__(self, name: str, attrib:list[Attr] =[], foreign: list[Foreign]=[], other=[]) -> None:
        self.name = name
        self.attrib = attrib
        self.foreign = foreign
        self.other = other

# Database class
"""
This class represents a MySQL database and provides methods for interacting with it.
"""
class Database:
    # Initializer
    def __init__(self, host="localhost", user="root", password="", database="") -> None:
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.db = mysql.connector.connect(
            host= host,
            user= user,
            password= password,
            database= database
        )
        self.cursor = self.db.cursor()

    # Create Table
    """
    This method creates a table in the database.
    """
    def create_table(self, table: Table):
        statement = f"CREATE TABLE IF NOT EXISTS {table.name} ( "
        statement += ",\n".join([f"{attribute.name} {attribute.type} {attribute.constraints}" for attribute in table.attrib])
        if len(table.foreign) > 0:
            statement += ", "
            statement += ",\n".join([f"FOREIGN KEY ({key.name}) REFERENCES {key.table}({key.refer})" for key in table.foreign])
        if len(table.other) > 0:
            statement += ", "
            statement += ", ".join(table.other)            
        statement += " );"
        self.cursor.execute(statement)
    
    # Drop Table
    """
    This method drops (deletes) a table from the database.
    """
    def drop_table(self, name=""):
        self.cursor.execute(f"DROP TABLE IF EXISTS {name}")
    
    # Show tables
    """
    This method shows all tables in the database.
    """
    def show_tables(self):
        tempcursor = self.cursor
        tempcursor.execute("SHOW TABLES")
        for data in tempcursor:
            print(data)

    # Insert into table
    """
    This method inserts data into a table.
    """
    def insert_to_table(self, table: Table, prim=[], data=[]):
        attributes = [attrib.name for attrib in table.attrib if attrib.name not in prim]
        query = f"INSERT INTO {table.name} ({', '.join(attributes)}) VALUES ({', '.join('%s' for i in range(len(attributes)))})"
        self.cursor.executemany(query, data)
        self.db.commit()
        print(self.cursor.rowcount, "row(s) were inserted.")
    
    # Select from table (single, multiple column or all)
    """
    This method retrieves data from a table.
    """
    def select_from(self, table: Table, columns=["*"], join="", cond=""):
        query = f"SELECT {', '.join(columns)} FROM {table.name}"
        query += f" {join}"
        if cond != "":
            query += f" WHERE {cond}"
        self.cursor.execute(query)
        return self.cursor.fetchall()

# CSV File reader
"""
This function reads data from a CSV file.
"""
def readCSV(fileName="", skipfirst=False):
    rows = []
    with open(fileName, mode='r', newline='') as csvfile:
        line_reader = csv.reader(csvfile)
        for row in line_reader:
            rows.append(row)
    rows = rows[1:] if skipfirst else rows
    return rows

DB = Database(host= HOST,
              user= USER, password= PASSWORD, database= DATABASE)

# Create Database 
prices_table =  Table(
    name="Prices",
    attrib= [
        Attr(name="price_id", type="INT", constraints="NOT NULL PRIMARY KEY AUTO_INCREMENT"),
        Attr("rental_price", "DECIMAL(5,2)", "NOT NULL CHECK (rental_price >= 0)")
    ]
)

books_table = Table(
    name= "Books",
    attrib= [
        Attr(name="book_id", type="INT", constraints="NOT NULL PRIMARY KEY AUTO_INCREMENT"),
        Attr("title", "VARCHAR(255)", "NOT NULL"),
        Attr("author","VARCHAR(255)", "NOT NULL"),
        Attr("genre", "VARCHAR(255)"),
        Attr("publication_year","YEAR"),
        Attr("price_id", "INT")
    ],
    foreign = [
        Foreign("price_id", "Prices", "price_id", "ON UPDATE CASCADE ON DELETE SET NULL")
    ]
)

rentals_table = Table(
    name= "Rentals",
    attrib= [
        Attr("rental_id", "INT", "NOT NULL PRIMARY KEY AUTO_INCREMENT"),
        Attr("book_id", "INT"),
        Attr("member_id", "INT", "NOT NULL"),
        Attr("rental_date", "DATE", "NOT NULL"),
        Attr("due_date", "DATE", "NOT NULL"),
        Attr("return_date", "DATE"),
    ],
    foreign = [
        Foreign("book_id", "Books", "book_id", "ON UPDATE CASCADE ON DELETE SET NULL")
    ],
    other= [
        "CONSTRAINT Chk_rental_date CHECK (return_date IS NULL OR return_date > rental_date)"
    ]
)

# DROP Tables if exist
print("Drop existing tables.")
DB.drop_table("Rentals")
DB.drop_table("Books")
DB.drop_table("Prices")
print()

# Create Prices Table
print("Create Prices table")
DB.create_table(prices_table)

# Create Books table
print("Create Books table")
DB.create_table(books_table)

# Create Rentals table
print("Create Rentals table")
DB.create_table(rentals_table)

print()

# Show all tables
print("Show all tables")
DB.show_tables()
print()

# Insert CSV data
print("Inserting from Prices.csv")
price_data = readCSV('Prices.csv', skipfirst=True)
DB.insert_to_table(table=prices_table, prim=['price_id'], data=price_data)

print("Inserting from Books.csv")
book_data = readCSV('Books.csv', skipfirst=True)
DB.insert_to_table(table=books_table, prim=['book_id'], data=book_data)

print("Inserting from Rentals.csv")
rental_data = readCSV('Rentals.csv', skipfirst=True)
DB.insert_to_table(rentals_table, prim=['rental_id'], data=rental_data)
print()

# Example select
result_0 = DB.select_from(table=books_table, columns=["title, publication_year"], cond="publication_year > 1999 AND publication_year < 2005 LIMIT 10")
print("Select All Book titles released between 1999 and 2005(Limit 10)")
for row in result_0:
    print(row)
print()

#Query 1
result_1 = DB.select_from(books_table, 
                          cond="price_id IN (select price_id FROM Prices WHERE rental_price > 50 ) LIMIT 10")
print("Select all books where the price is greater than 50(Limit 10):")
for row in result_1:
    print(row)
print()

#Query 2
result_2 = DB.select_from(books_table, ["Books.*", "Prices.rental_price"],
                          join="JOIN Prices ON Books.price_id = Prices.price_id",
                          cond = "Prices.price_id IN (SELECT Prices.price_id FROM Prices WHERE rental_price > 50 ) LIMIT 10")
print("If we want to see the books and the prices as well(limit 10):")
for row in result_2:
    print(row)
print()

#Query 3
result_3 = DB.select_from(rentals_table, cond="book_id = 53")
print("Return all rental record of a certain book:")
for row in result_3:
    print(row)
print()