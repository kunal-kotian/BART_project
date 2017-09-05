# MSAN 691 HW # 3: BART Data
# Sep 5, 2017

import os
import re
import pandas as pd
import zipfile
import xlrd
import psycopg2


# Function definitions -------------------------------------------------------
def get_filepaths(root):
    """return list of fully qualified paths of files in root (directory)"""
    filepaths = []
    for dirpath, dirs, files in os.walk(root):
        for fname in files:
            if not fname.startswith('.'):           # Ignore hidden files
                filepaths += [os.path.join(dirpath, fname)]
    return filepaths


def unzip_files(zip_dir, unzip_dir):
    """(i)  unzip all files in zip_dir and store them in unzip_dir
       (ii) return list containing fully qualified paths of unzipped files
    """
    filepaths = get_filepaths(zip_dir)
    # unzip & save zipped files from input dir in unzipDir
    for file in filepaths:
        zipfile.ZipFile(file, 'r').extractall(unzip_dir)
        zipfile.ZipFile(file, 'r').close()
    return get_filepaths(unzip_dir)


def get_month_n_year(string):
    """return month (int) and year (int) extracted from a string containing a
    month (name) and a year (numeric)
    """
    all_months = ('january', 'february', 'march', 'april', 'may', 'june',
                  'july', 'august', 'september', 'october', 'november',
                  'december')
    month = [ind for ind, month in enumerate(all_months) if month in
             string.lower()]
    month[0] = month[0] + 1
    year = re.findall('\d+', string)
    return month[0], int(year[0])


def extract_data(data_file):
    """Takes in data_file, a fully qualified path of a file with BART data,
    and returns a data frame with the ridership data in long table format
    """
    my_workbook = xlrd.open_workbook(data_file)
    df = []  # Initialize data frame to store the extracted ridership data
    header = []
    for sheet in my_workbook.sheets():
        try:
            daytype = (sheet.cell(0, 3).value).lower()   # weekday identifier
        except:
            # if daytype is not a string, we're on the wrong sheet
            break
        if 'weekday' in daytype:
            daytype = 'weekday'
        elif 'saturday' in daytype:
            daytype = 'saturday'
        elif 'sunday' in daytype:
            daytype = 'sunday'
        else:
            break
        # Limiting column index: left of the column containing the term 'Exits'
        col_limit = sheet.row_values(1).index('Exits') - 1
        # Limiting row index: above the row containing the term 'Entries'
        row_limit = sheet.col_values(0).index('Entries') - 1
        if len(header) == 0:
            header_cells = sheet.row_slice(rowx=1, start_colx=0,
                                           end_colx=(col_limit + 1))
            header = [cell.value for cell in header_cells]
        # Read in ridership data
        for row_index in range(2, row_limit + 1):
            row_cells = sheet.row_slice(rowx=row_index, start_colx=0,
                                        end_colx=col_limit + 1)
            row_data = []
            for cell in row_cells:
                row_data += [cell.value]
            df += [[daytype] + row_data]         # Add daytype to each record
    header[0] = 'Exit stations'   # Specify col name in empty header location
    header.insert(0, 'daytype')
    df = pd.DataFrame(df)
    df.columns = header
    # Add columns showing month and year
    month, year = get_month_n_year(os.path.basename(data_file))
    df['mon'] = month
    df['year'] = year
    # Convert the dataframe from wide to long format
    df = pd.melt(df, id_vars=['mon', 'year', 'daytype', 'Exit stations'],
                     var_name='Entry stations', value_name='riders')
    df = df[['mon', 'year', 'daytype', 'Entry stations', 'Exit stations',
             'riders']]
    return df


def make_postgres_table(sql_cursor, schema, table):
    """Creates a table in postgres using the information provided"""
    sql_table_exists = "SELECT EXISTS (SELECT 1 FROM pg_tables " \
                       "WHERE schemaname = '%s' AND tablename = '%s');" \
                       % (schema, table)
    sql_make_table = "CREATE TABLE %s.%s(" \
                     "mon INT, yr INT, daytype VARCHAR(15), " \
                     "start VARCHAR(5), term VARCHAR(5), riders FLOAT);" \
                     % (schema, table)
    sql_drop_table = "DROP TABLE %s.%s;" % (schema, table)
    sql_cursor.execute(sql_table_exists)
    table_status = sql_cursor.fetchone()[0]
    # If the table already exists in the database, we drop it
    if table_status:
        sql_cursor.execute(sql_drop_table)
    # Create the required table in the specified schema
    sql_cursor.execute(sql_make_table)


def get_postgres_columns(sql_cursor, schema, table):
    """Returns a list of the names of all columns in a postgres table"""
    sql_col_names = "SELECT * FROM %s.%s" % (schema, table)
    sql_cursor.execute(sql_col_names)
    postgres_colnames = [desc[0] for desc in sql_cursor.description]
    return postgres_colnames


def write_to_file(dir_name, filepaths_all, output_csv='toLoad.csv'):
    """Write all of the ridership data to a temporary csv file
    :param dir_name: path of directory where output_csv is created
    :param filepaths_all: list containing paths of unzipped data files
    :param output_csv: name of the temporary csv file with ridership data
    """
    df_all = []
    for filepath in filepaths_all:
        df_all += [extract_data(filepath)]
    df_unified = pd.concat(df_all, axis=0)
    df_unified.to_csv(os.path.join(dir_name, output_csv), index=False)


def send_to_postgres(dir_name, sql_cursor, schema, table, columns,
                     csv_file='toLoad.csv'):
    """Send the ridership data from csv_file to a postgres table
    :param dir_name: path of directory where output_csv is created
    :param sql_cursor: SQL connection cursor
    :param schema, table: names of the target schema and the table in postgres
    :param columns: list of the names of all columns in the postgres table
    :param csv_file: name of the temporary csv file with ridership data
    """
    csv_filepath = os.path.join(dir_name, csv_file)
    sql_send_to_table = "COPY %s.%s (%s, %s, %s, %s, %s, %s) FROM '%s' WITH " \
                        "(FORMAT CSV, HEADER True);" % \
                        tuple([schema] + [table] + columns + [csv_filepath])
    sql_cursor.execute(sql_send_to_table)
    os.remove(csv_filepath)


def ProcessBart(tmpDir, dataDir, SQLConn=None, schema='cls', table='bart'):
    """Clean and organize the BART ridership data and send it to a SQL db"""
    # Part 1: Collecting and preparing data ---------------------------------
    # Get a list of the paths of all unzipped Excel files
    filepaths_unzipped = unzip_files(dataDir, tmpDir)
    # Extract ridership data, reshape it, and write it to a single csv file
    write_to_file(tmpDir, filepaths_unzipped)
    if SQLConn is not None:
        # Part 2: Create the specified postgres table for BART data ---------
        SQLConn.autocommit = True
        sql_cursor = SQLConn.cursor()
        make_postgres_table(sql_cursor, schema, table)
        postgres_columns = get_postgres_columns(sql_cursor, schema, table)
        # Part 3: Enter the BART data into the postgres table ---------------
        send_to_postgres(tmpDir, sql_cursor, schema, table, postgres_columns)
        # Clean up temporary file and close out SQL connection
        SQLConn.commit()
        SQLConn.rollback()
        SQLConn.close()


# Tests--------------------------------------------------------------------
# zipped files here:
dataDir = '/Users/kunal/Desktop/MSAN_Coursework/691/' \
                       'Homeworks/HW3/bart_data'
# unzipped files here:
tmpDir = '/Users/kunal/Desktop/MSAN_Coursework/691/' \
                         'Homeworks/HW3/unzipped'
# postgres connection:
LCLconnR = psycopg2.connect("dbname='msan_691' user='postgres' "
                            "host='localhost' password='postgres'")
ProcessBart(tmpDir=tmpDir,
            dataDir=dataDir,
            SQLConn=LCLconnR,
            schema='cls', table='bart')
print 'Done.'