#!/usr/bin/python
"""
    The objective of this script is compare the data from two databases,
        and the steps to follow are the following:
    - Connect to two databases (backoffice and aws)
    - Identify the tables and fields that must be the same
    - Compare the raw row count in both tables, that should match
    - If the row count are the same, next step should do a sanity check on the
        data, then currency check
    - Write couple query to run in under a min.
"""

import json
import psycopg2
import requests
import pandas
import click
import logging
import json_log_formatter
import dbconfig
import datacompy


config = dbconfig.read_yaml('conf.yaml')

formatter = json_log_formatter.JSONFormatter()
json_handler = logging.FileHandler(filename=config['logs'][0]['path'])
json_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.addHandler(json_handler)
logger.setLevel(logging.INFO)


def get_tables(file: str = 'tables.json') -> dict:
    """ Get the list of the tables availables to compare,
        and some metadata to do the comparison
    """

    with open(file, encoding='utf-8') as json_file:
        data = json.load(json_file)
    return data


def get_data_from_db(resource: str, query: str) -> pandas.DataFrame:
    """ Establish a connection to the database by creating a cursor object """

    params = dbconfig.load_config(config['global']['dbconf_path'], resource)
    conn = psycopg2.connect(**params)
    cur = conn.cursor()

    dataset = pandas.read_sql_query(query, conn)

    cur.close()
    conn.close()

    return dataset


def get_count(table: str, conditions: str) -> int:
    """ Get the count(*) of a table.
            Use resultset['count'][0]
    """
    where = 'true' if conditions == '' else conditions
    return f"SELECT count(*) FROM {table} WHERE {where}"


def get_sql_query(table: str, cols: str, conditions: str) -> str:
    """ Creates a SQL query """
    columns = '*' if len(cols) == 0 else ','.join(cols)
    where = 'true' if conditions == '' else conditions
    return f"SELECT {columns} FROM {table} WHERE {where}"


def format(table: str, report: str) -> dict:
    head = '\n\n' + '%' * len(table) + '\n' + table.upper() + '\n' +  '%' * len(table)
    report = report.replace('DataComPy Comparison\n--------------------', head)
    return '{"text": "' + report + '"}'


def to_slack(url: str, body: dict) -> requests.Response:
    """ Sends a message summary to slack """

    headers = {"Content-type": "application/json"}
    return requests.post(url, data=body, headers=headers)


@click.command()
@click.option('--columns', '-c', default=None, multiple=False, type=str,
    help='List of comma separated columns e.g. "id,payoff_uid,created_date"')
@click.option('--describe', '-d',  multiple=False,
    help='Describe details of a table to be compared')
@click.option('--table', '-t', multiple=False,
    help='Name of table to be compared')
@click.option('--where', '-w', default=None, multiple=False,
    help='Where clause for the SQL query')
@click.option('--environments', '-e', default=('localdb', 'postgres-db'),
    multiple=True, help='Name of the two environments to be compared')
def cli(columns, describe, table, where, environments):
    """Command line interface to define the tables to be compared"""

    # Read metadata of the tables
    meta = get_tables()
    # Get table names
    tables = list(meta.keys())

    if table == None and describe == None:
        click.echo('Available tables:')
        for t_name in tables:
            click.echo(f'\t{t_name}')
        return
    
    if describe:
        cols = ','.join(meta[describe]["columns"])
        click.echo(f'Table:\n\t{describe}')
        click.echo(f'Where condition:\n\t{meta[describe]["conditions"]}')
        click.echo(f'Columns:\n\t{cols}')

    if table in tables:
        print(table)
        logger.info(f'[{table}] table comparison')
        cols = columns if columns != None else meta[table]['columns']
        condition = where if where != None else meta[table]['conditions']

        sql_query = get_sql_query(table, cols, condition)
        logger.info(f'[{table}] {sql_query}')

        src_dataframe = get_data_from_db(environments[0], sql_query)
        #dst_dataframe = get_data_from_db(environments[1], sql_query.replace('vault_backend.content_resource', 'vault_backend.content_resource_copy'))
        dst_dataframe = get_data_from_db(environments[1], sql_query)

        compare = datacompy.Compare(
            src_dataframe,
            dst_dataframe,
            join_columns=meta[table]['joinColumns'],
            abs_tol=0,
            rel_tol=0,
            df1_name=environments[0],
            df2_name=environments[1]
        )
        if compare.matches(ignore_extra_columns=False):
            logger.info(f'[{table}] Table match!')
            summary = {"text": f'[{table}] Table match!'}
        else:
            logger.info(f'[{table}] mismatch')
            summary = format(table, compare.report())
        
        #to_slack(config['global']['webhook_path'], summary)
        print(summary)


