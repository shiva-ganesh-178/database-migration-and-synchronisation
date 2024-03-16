import psycopg2
import mysql.connector

def create_table():
    mysql_conn = mysql.connector.connect(
        host='localhost',
        username='root',
        password='lakshyam'
    )

    mysql_cur = mysql_conn.cursor()

    mysql_cur.execute('USE db1')
    mysql_cur.execute('CREATE TABLE persons(id int AUTO_INCREMENT primary key, name varchar(30))')

    mysql_conn.commit()
    mysql_cur.close()
    mysql_conn.close()

# create_table()


def insert_data(add_data):
    mysql_conn = mysql.connector.connect(
        host='localhost',
        username='root',
        password='lakshyam'
    )

    mysql_cur = mysql_conn.cursor()

    mysql_cur.execute('USE db1')

    for person in add_data:
        mysql_cur.execute('INSERT INTO persons(name) VALUES(%s)',person)

    mysql_conn.commit()
    mysql_cur.close()
    mysql_conn.close()


add_data = [('Shiva',),('Ganesh',),('Mahesh',),('Jay',),('Vedha',)]
# add_data = [('Barath',),('Bhargav',),('Adil',),('Chandan',),('Uday Mama',)]

# insert_data(add_data)


def copy_to_postgres():
    mysql_conn = mysql.connector.connect(
        host='localhost',
        username='root',
        password='lakshyam'
    )

    mysql_cur = mysql_conn.cursor()

    mysql_cur.execute('USE db1')

    mysql_cur.execute('SELECT * FROM persons')

    mysql_data=mysql_cur.fetchall()

    postgres_conn = psycopg2.connect(
        host='localhost',
        user='postgres',
        password='*****',
        database='assignment'
    )

    postgres_cur = postgres_conn.cursor()

    postgres_cur.execute('CREATE TABLE IF NOT EXISTS persons(id int,name varchar(30))')

    for person in mysql_data:
        postgres_cur.execute('INSERT INTO persons(id,name) VALUES(%s,%s)',person)

    postgres_conn.commit()

    postgres_cur.close()
    postgres_conn.close()

# copy_to_postgres()

def updation():
    mysql_conn = mysql.connector.connect(
        host='localhost',
        username='root',
        password='******'
    )

    mysql_cur = mysql_conn.cursor()

    mysql_cur.execute('USE db1')

    mysql_cur.execute('SELECT * FROM persons')

    mysql_data = mysql_cur.fetchall()

    print(mysql_data)
    postgres_conn = psycopg2.connect(
        host='localhost',
        user='postgres',
        password='******',
        database='assignment'
    )

    postgres_cur = postgres_conn.cursor()

    postgres_cur.execute('SELECT * FROM persons')

    postgres_data = postgres_cur.fetchall()

    print(mysql_data)
    print(postgres_data)
    mysql_dict = [{'id': i[0], 'name': i[1]} for i in mysql_data]
    postgres_dict = [{'id': i[0], 'name': i[1]} for i in postgres_data]

    for i in mysql_dict:
        # print(i)
        edokati = False
        for j in postgres_dict:
            # print(j.values())
            if i['id'] == j['id']:
                edokati = True
                if i['name'] != j['name']:
                    postgres_cur.execute('update persons set name = %s where id = %s',
                                         (i['name'], i['id']))
                break
        if not edokati:
            postgres_cur.execute('insert into persons (id, name) values (%s, %s)',(i['id'], i['name']))

    mysql_cur.close()
    mysql_conn.close()

    postgres_conn.commit()
    postgres_cur.close()
    postgres_conn.close()


updation()