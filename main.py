
import sqlite3
from sqlite3 import Error
from random import *
import random
from faker import Faker
fake = Faker('pl_PL')

def create_connection(db_file):
   """ create a database connection to the SQLite database
   specified by db_file
   :param db_file: database file
   :return: Connection object or None
   """
   conn = None
   try:
      conn = sqlite3.connect(db_file)
      print(f"Connected to {db_file}, sqlite version: {sqlite3.version}")
   except Error as e:
      print(e)
   return conn

def execute_sql(conn, sql): 
   """ Execute sql
   :param conn: Connection object
   :param sql: a SQL script
   :return:
   """
   try:
      c = conn.cursor()
      c.execute(sql)
      c.close()
   except Error as e:
      print(e)

def add_company(conn, company):
   """
   Create a new company into the company table
   :param conn:
   :param company:
   :return: company id
   """
   sql = '''INSERT INTO company(company_name, adress, NIP, company_type, first_name, last_name, phone, email, position)
            VALUES(?,?,?,?,?,?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, company)
   conn.commit()
   return cur.lastrowid

def add_contract(conn, contract):
   """
   Create a new contract into the contract table
   :param conn:
   :param contract:
   :return: contract id
   """
   sql = '''INSERT INTO contracts(company_ID, year, contract_short_name, value, start_date, end_date, status, contract_description, location)
            VALUES(?,?,?,?,?,?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, contract)
   conn.commit()
   return cur.lastrowid

def select_all(conn, table):
  """
  Query all rows in the table
  :param conn: the Connection object
  :param table: table name
  :return:
  """
  cur = conn.cursor()
  cur.execute(f"SELECT * from {table}")
  rows = cur.fetchall()
  return rows

def select_where(conn, table, **query):
  """
  Query tasks from table with data from **query dict
  :param conn: the Connection object
  :param table: table name
  :param query: dict of attributes and values
  :return:
  """
  cur = conn.cursor()
  qs = []
  values = ()
  for k, v in query.items():
   qs.append(f"{k}=?")
   values += (v,)
  q = " AND ".join(qs)
  cur.execute(f"SELECT * from {table} WHERE {q}", values)
  rows = cur.fetchall()
  return rows

def update(conn, table, id, **kwargs):
  """
  update status, begin_date, and end date of a task
  :param conn:
  :param table: table name
  :param id: row id
  :return:
  """
  parameters = [f"{k} = ?" for k in kwargs]
  parameters = ", ".join(parameters)
  values = tuple(v for v in kwargs.values())
  values += (id, )

  sql = f''' UPDATE {table}
         SET {parameters}
         WHERE id = ?'''
  try:
      cur = conn.cursor()
      cur.execute(sql, values)
      conn.commit()
      print(f"Update {values}")
  except sqlite3.OperationalError as e:
      print(e)

def delete_all(conn, table):
  """
  Delete all rows in the table
  :param conn: the Connection object
  :param table: table name
  :return:
  """
  cur = conn.cursor()
  cur.execute(f"DELETE from {table}")
  conn.commit()
  print("Deleted")

def delete_where(conn, table, **query):
  """
  Delete from table with data from **query dict
  :param conn: the Connection object
  :param table: table name
  :param query: dict of attributes and values
  :return:
  """
  cur = conn.cursor()
  qs = []
  values = ()
  for k, v in query.items():
   qs.append(f"{k}=?")
   values += (v,)
  q = " AND ".join(qs)
  cur.execute(f"DELETE from {table} WHERE {q}", values)
  conn.commit()
  print("Deleted")

create_company_sql = """
   -- company table
   CREATE TABLE IF NOT EXISTS company (
      id integer PRIMARY KEY,
      company_name text NOT NULL UNIQUE,
      adress text NOT NULL,
      NIP integer NOT NULL UNIQUE,
      first_name text NOT NULL,
      last_name text NOT NULL,
      phone text NOT NULL UNIQUE,
      email text NOT NULL UNIQUE,
      position text,
      company_type text CHECK(company_type IN ('prywatne', 'państwowe'))
   );
   """
create_contracts_sql = """
   -- contracts table
   CREATE TABLE IF NOT EXISTS contracts (
      id integer PRIMARY KEY,
      company_ID integer NOT NULL,
      year integer NOT NULL,
      contract_short_name text NOT NULL,
      value REAL NOT NULL,
      start_date DATE,
      end_date DATE,
      status text CHECK(status IN ('ofertowany', 'realizowany', 'zakończony')) DEFAULT 'ofertowany',
      contract_description text, 
      location text NOT NULL,
      FOREIGN KEY (company_ID) REFERENCES company (ID)
   );
   """

def create_company():
   company01 = ("Grupa Budimex S.A.", "ul. Stawki 40, 01-040 Warszawa", 5260212167, "prywatne", fake.first_name(), fake.last_name(), fake.phone_number(), fake.email(), fake.job())
   company_id = add_company(conn, company01)
   print(f"Dodano firmę o indeksie {company_id}")
   company02 = ("Grupa Strabag Sp. z o.o.", "ul. Parzniewska 10, 05-800 Pruszków", 5260210686, "prywatne", fake.first_name(), fake.last_name(), fake.phone_number(), fake.email(), fake.job())
   company_id = add_company(conn, company02)
   print(f"Dodano firmę o indeksie {company_id}")
   company03 = ("Grupa PORR S.A.", "ul. Hołubcowa 123, 02-854 Warszawa", 5260210253, "prywatne", fake.first_name(), fake.last_name(), fake.phone_number(), fake.email(), fake.job())
   company_id = add_company(conn, company03)
   print(f"Dodano firmę o indeksie {company_id}")
   company04 = ("Grupa Erbud S.A.", "ul. Franciszka Klimczaka 1, 02-797 Warszawa", 8790172106, "prywatne", fake.first_name(), fake.last_name(), fake.phone_number(), fake.email(), fake.job())
   company_id = add_company(conn, company04)
   print(f"Dodano firmę o indeksie {company_id}")
   company05 = ("Grupa Unibep S.A.", "ul. 3 Maja 19, 17-100 Bielsk Podlaski", 5430200369, "prywatne", fake.first_name(), fake.last_name(), fake.phone_number(), fake.email(), fake.job())
   company_id = add_company(conn, company05)
   print(f"Dodano firmę o indeksie {company_id}")
   company06 = ("Warbud S.A.", "ul. Domaniewska 32, 02-672 Warszawa", 5261029050, "prywatne", fake.first_name(), fake.last_name(), fake.phone_number(), fake.email(), fake.job())
   company_id = add_company(conn, company06)
   print(f"Dodano firmę o indeksie {company_id}")
   company07 = ("Grupa Polimex Mostostal S.A.", "Al. Jana Pawła II 12, 00-124 Warszawa", 8210045937, "państwowe", fake.first_name(), fake.last_name(), fake.phone_number(), fake.email(), fake.job())
   company_id = add_company(conn, company07)
   print(f"Dodano firmę o indeksie {company_id}")
   company08 = ("Grupa Torpol S.A.", "ul. Mogileńska 10G, 61-052 Poznań", 9720900396, "prywatne", fake.first_name(), fake.last_name(), fake.phone_number(), fake.email(), fake.job())
   company_id = add_company(conn, company08)
   print(f"Dodano firmę o indeksie {company_id}")
   company09 = ("Mostostal Warszawa S.A.", "ul. Konstruktorska 12A, 02-673 Warszawa", 5260204995, "prywatne", fake.first_name(), fake.last_name(), fake.phone_number(), fake.email(), fake.job())
   company_id = add_company(conn, company09)
   print(f"Dodano firmę o indeksie {company_id}")
   company10 = ("Grupa Trakcja S.A.", "ul. Złota 59, 00-120 Warszawa", 5250014547, "prywatne", fake.first_name(), fake.last_name(), fake.phone_number(), fake.email(), fake.job())
   company_id = add_company(conn, company10)
   print(f"Dodano firmę o indeksie {company_id}")

def create_contracts_automat():
   status = ["ofertowany", "realizowany", "zakończony"]
   for i in range(50):
      contract = (randint(1,10), randint(2020, 2025), f"Budowa " + fake.company(), round(random.uniform(100000,5000000), 2), str(fake.date_between(start_date='-2y', end_date='today')), str(fake.date_between(start_date='+6m', end_date='+2y')), random.choice(status), fake.catch_phrase(), fake.city() )
      contract_id = add_contract(conn, contract)
      print(f"Dodano kontrakt o indeksie {contract_id}")

if __name__ == '__main__':
   conn = create_connection(r"database.db")
   if conn:
      execute_sql(conn, create_company_sql)
      execute_sql(conn, create_contracts_sql)
      create_company()
      create_contracts_automat()

      print("\nTABELA 'COMPANY': ")
      sel01 = select_all(conn, "company")
      for comp in sel01:
         print(comp)

      print("\nTABELA 'CONTARCTS': ")
      sel02 = select_all(conn, "contracts")
      for cont in sel02:
         print(cont)

      print("\nFirma ID = 7: ")
      sel03 = select_where(conn, "company", ID = 7)
      for comp in sel03:
         print(comp)

      print("\nKONTRAKTY FIRMY company_ID = 3 ze statusem 'ofertowany': ")
      sel04 = select_where(conn, "contracts", company_ID = 3, status = "ofertowany")
      for cont in sel04:
         print(cont)

      for i in range(41, 51): #zakres ID
         update(conn, "contracts", i, status = "zakończony" )

      print("Usuwam wszystkie kontrakty zawarte w 2023r.")
      delete_where(conn, "contracts", year = 2023)

      #print("Usuwam dane z tabeli 'contracts")
      #delete_all(conn, "contracts")

      conn.close()
   else:
      print("Problem z połączeniem z bazą danych")


"""
    with sqlite3.connect("database.db") as conn:
      # wywołanie funkcji
"""