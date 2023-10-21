#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):

    cur = openconnection.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS {0} (userid int, t1 char, movieid int, t2 char, rating float, t3 char, t4 varchar, PRIMARY KEY(userid, movieid), CHECK(rating>=0 AND rating<=5))'.format(ratingstablename)
)

    # print('Table {} created'.format(ratingstablename))

    cur.execute(''' COPY {0} FROM '{1}' DELIMITERS ':' '''.format(ratingstablename, ratingsfilepath))

    # print('Data inserted into Table {}'.format(ratingstablename))

    cur.execute('ALTER TABLE {} DROP COLUMN t1, DROP COLUMN t2, DROP COLUMN t3, DROP COLUMN t4'.format(ratingstablename))

    cur.execute('CREATE TABLE Metadata (row_count int, range_partitions int, rr_partitions int)')
    cur.execute('INSERT INTO Metadata (SELECT COUNT(*), 0, 0 FROM {0})'.format(ratingstablename))

    # print('Metadata table created')
    
    openconnection.commit()
    cur.close()
    openconnection.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    pass


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    pass

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
        # print ('DB {} created'.format(dbname))
        con.close()

        con = getOpenConnection(dbname=dbname)
        loadRatings(ratingstablename='Ratings', ratingsfilepath='./text_data.txt', openconnection=con)
    else:
        print ('A database named {0} already exists'.format(dbname))
        cur.execute('DROP DATABASE %s' % (dbname,))
        con.close()

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print ('Error %s' % e)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print ('Error %s' % e)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    createDB()