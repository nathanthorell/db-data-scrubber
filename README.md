# db-data-scrubber

Basic data scrubbing scripts to run against a SQL database.  Uses [Faker](https://github.com/joke2k/faker)

## Local Env Setup

1. `python -m venv .venv/`
1. `source .venv/bin/activate`
1. `python -m pip install -r ./requirements.txt`

  - Note: on Apple Silicon use `brew install unixodbc` and `pip install --no-binary :all: pyodbc==4.0.39`
  - Also [https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver16#microsoft-odbc-18]

## Configurations

- Add any database mappings in the `config.json` file
- data_type in the config refers not to the SQL datatype (varchar) but rather the specific fake data type
- See the list that faker uses: [https://faker.readthedocs.io/en/master/providers.html]

## Requirements

1. Environment Variables
    1. DB_DRIVER
    1. DB_SERVER
    1. DB_PORT
    1. DB_DATABASE
    1. DB_USER
    1. DB_PASS
    1. DB_ENCRYPT
