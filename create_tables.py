import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Removes any data and structure of the tables which will be used next 
    
    Arguments:
        cur {psycopg2 cursor } -- Cursor to execute querys vs database
        conn {psycopg2 connection} -- Connection to RedShift DataBase
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Creates structure of the tables which will be used next
    
    Arguments:
        cur {psycopg2 cursor } -- Cursor to execute querys vs database
        conn {psycopg2 connection} -- Connection to RedShift DataBase
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()