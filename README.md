# DB Data comparator
This script helps to compare two tables from different databases, and could be useful in the migration of databases. Provides a summary of the differences, using `datacompy` library.

## Comparisson between tables

All the tables defined in the `tables.json` table can be compared

The _columns_ to be compared are in the tables.json, but these column names can be sent as arguments in command line.
As columns, the _where_ condition as well is defined in tables.json, but it can be sent as argument in the command line

## Install

Create virtualenv and install packages:
  - Install pip `pip3 install virtualenv`
  - Create the virtual environment: `virtualenv venv`
  - Activate the virtual environment `. venv/bin/activate`
  - Install packages: `pip install -r requirements.txt`
  - Help: `python3 cmp --help`
  - List tables: `python3 cmp`
  - Run comparison: `python3 cmp -t <TABLE_NAME>`

### Configuration

The properties related to the database connection must be defined in a file called `database.ini`.
The Webhook URL must be defined.



## Run
To execute the comparison, you need to define a list of tables to be compared:
```
python dbcomparer -t <TABLE_NAME>
```