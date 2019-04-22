import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Loads in Staging Tables by S3 resources
    
    Arguments:
        cur {psycopg2 cursor } -- Cursor to execute querys vs database
        conn {psycopg2 connection} -- Connection to RedShift DataBase
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Inserts in non Staging Tables data from Staging Tables 
    
    Arguments:
        cur {psycopg2 cursor } -- Cursor to execute querys vs database
        conn {psycopg2 connection} -- Connection to RedShift DataBase
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()