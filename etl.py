import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        extracts and loads data in s3 bucket file to staging tables by 
        running queries in "copy_table_queries" list that uses COPY command
        
        INPUT:
        - cur: cursor variable of the database
        - conn: connection variable of the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        inserts staging tables data into fact and dimentions tables
        by running queries in the list "insert_table_queries"
        
        INPUT:
        - cur: cursor variable of the database
        - conn: connection variable of the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connectes to the database using values in the dwh.cfg file
    performs ETL to load data into staging tables 
    and then to final tables created for analytics
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Loading s3 bucket files to staging tables... ')
    load_staging_tables(cur, conn)
        
    print('inserts staging tables data into fact and dimentions tables... ')
    insert_tables(cur, conn)

    conn.close()
    
    print('ETL successfully completed')

if __name__ == "__main__":
    main()