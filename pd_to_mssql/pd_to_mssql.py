import pandas as pd
import pyodbc
from queue import Queue
from threading import Thread
import math
from distutils.util import strtobool


class TruncationException(Exception):
    pass


class NullValueException(Exception):
    pass


class MissingColumnsException(Exception):
    pass


class SQLException(Exception):
    pass


def task(table_name, schema, cnxn_string, df_queue, columns, ignore_truncation, cursors, exceptions):
    column_select_list = ', '.join(columns['SELECT_SAFE_COLUMN_NAME'])
    column_specification = '(' + column_select_list + ')'

    cnxn = pyodbc.connect(cnxn_string)
    crsr = cnxn.cursor()

    if ignore_truncation:
        crsr.execute('SET ANSI_WARNINGS OFF')

    crsr.execute(f'SELECT {column_select_list} INTO #TEMP FROM {schema}.{table_name} WHERE 1=0')
    crsr.commit()

    try:
        while not df_queue.empty():
            df = df_queue.get()
            insert_statement = f'INSERT INTO #TEMP {column_specification} VALUES\n'

            for i, row in df.iterrows():
                insert_line = '('
                for column, data_type in zip(columns['COLUMN_NAME'], columns['DATA_TYPE']):
                    # Handle NULLs first
                    if pd.isnull(row[column]):
                        insert_line += 'NULL, '
                    # ANSI char type
                    elif data_type in ('varchar', 'char', 'text', 'date', 'datetime2', 'datetime', 'datetimeoffset', 'smalldatetime', 'time'):
                        insert_line += f"'{row[column]}', "
                    # Unicode
                    elif data_type in ('nvarchar', 'nchar', 'ntext'):
                        insert_line += f"N'{row[column]}', "
                    # Numeric
                    elif data_type in ('bigint', 'decimal', 'int', 'money', 'numeric', 'smallint', 'smallmoney',
                                       'tinyint', 'float', 'real'):
                        insert_line += row[column] + ', '
                    elif data_type == 'bit':
                        insert_line += str(strtobool(row[column])) + ', '
                insert_statement += insert_line[:-2] + '),\n'
            insert_statement = insert_statement[:-2]
            crsr.execute(insert_statement)
            crsr.commit()
    except Exception as e:
        exceptions.put(e)
        return

    cursors.put(crsr)


def thread_manager(table_name, schema, cnxn_string, thread_count, df_queue, columns, ignore_truncation):
    cursors = Queue()
    exceptions = Queue()
    threads = [Thread(target=task, args=(table_name, schema, cnxn_string, df_queue, columns, ignore_truncation, cursors, exceptions)) for i in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    if exceptions.qsize() > 0:
        exception_out = '\n'
        while not exceptions.empty():
            exception_out += str(exceptions.get()) + '\n'
        raise SQLException(exception_out)

    column_select_list = ', '.join(columns['SELECT_SAFE_COLUMN_NAME'])
    column_specification = '(' + column_select_list + ')'
    while not cursors.empty():
        crsr = cursors.get()
        crsr.execute(f"""INSERT INTO {schema}.{table_name} {column_specification}
                     SELECT {column_select_list} FROM #TEMP
                     DROP TABLE #TEMP""")
        crsr.commit()
        crsr.close()


def to_sql(df_in, table_name, cnxn_string, schema='dbo', index=True, replace=False, chunk_size=1000, thread_count=5, ignore_truncation=False, ignore_missing=False):
    # Make a copy of the data, as to not apply adjustments to the original dataframe
    df_out = df_in.copy()

    # table columns
    trimmed_name = table_name.replace('[', '').replace(']', '')
    query = f"""SELECT *,
    COLUMNPROPERTY(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IDENTITY_FLAG
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{trimmed_name}'
    AND TABLE_SCHEMA = '{schema}'
    ORDER BY ORDINAL_POSITION"""
    df_table = pd.read_sql(query, pyodbc.connect(cnxn_string))
    df_table['SELECT_SAFE_COLUMN_NAME'] = df_table['COLUMN_NAME'].apply(lambda x: '[' + str(x) + ']')
    columns = df_table[df_table['IDENTITY_FLAG'] == 0][['COLUMN_NAME', 'SELECT_SAFE_COLUMN_NAME', 'DATA_TYPE', 'IS_NULLABLE', 'CHARACTER_MAXIMUM_LENGTH']]

    # Check for common insertion errors
    # Missing columns
    missing_cols = set(columns['COLUMN_NAME']) - set(df_out.columns)
    if not ignore_missing:
        if len(missing_cols) > 0:
            raise MissingColumnsException(f'The following columns are missing from the input dataframe: \n{missing_cols}\n'
                                          'If you would like these columns to be filled with nulls, set the keyword parameter ignore_missing=True')
    else:
        for col in missing_cols:
            df_out[col] = None

    # Null values exist in a non-nullable column
    non_null_cols = columns[columns['IS_NULLABLE'] == 'NO']['COLUMN_NAME']
    for col in non_null_cols:
        if df_out[col].isnull().max():
            raise NullValueException(f"Column '{col}' contains null values but is a non-nullable column on the sql server")

    # truncation will occur on character based data types
    if not ignore_truncation:
        max_char_cols = columns[(~columns['CHARACTER_MAXIMUM_LENGTH'].isnull()) & (columns['CHARACTER_MAXIMUM_LENGTH'] != -1)][['COLUMN_NAME', 'CHARACTER_MAXIMUM_LENGTH']]
        for col, max_char in zip(max_char_cols['COLUMN_NAME'], max_char_cols['CHARACTER_MAXIMUM_LENGTH']):
            if df_out[col].apply(lambda x: len(str(x)) if not pd.isnull(x) else 0).max() > max_char:
                raise TruncationException(f"Column '{col}' contains elements that are too large for the destination table.\n"
                                          'To avoid this error and allow string data truncation, set the keyword parameter ignore_truncation=True')

    if replace:
        cnxn = pyodbc.connect(cnxn_string)
        crsr = cnxn.cursor()
        crsr.execute(f"DELETE FROM {schema}.{table_name}")
        crsr.commit()
        crsr.close()
        cnxn.close()

    if index:
        df_out.reset_index(inplace=True)
    else:
        df_out.reset_index(inplace=True, drop=True)

    # stringify
    for column in df_out.columns:
        df_out[column] = df_out[column].apply(lambda x: str(x).replace("'", "''") if not pd.isnull(x) else x)
        df_out[column] = df_out[column].apply(lambda x: x.replace('\n', '\\n') if not pd.isnull(x) else x)

    chunk_count = math.ceil(float(len(df_out.index))/chunk_size)

    # make sure the thread count is not greater than the chunk count
    thread_count = min(thread_count, chunk_count)

    output_dfs = [pd.DataFrame() for i in range(chunk_count)]
    for i in range(len(output_dfs)):
        output_dfs[i] = df_out[(i * chunk_size):((i+1) * chunk_size if (i+1) * chunk_size <= len(df_out.index) else None)]

    df_queue = Queue()
    for df in output_dfs:
        df_queue.put(df)

    thread_manager(table_name, schema, cnxn_string, thread_count, df_queue, columns, ignore_truncation)
