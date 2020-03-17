import pandas as pd
import numpy as np
import pyodbc
from multiprocessing import Process, Queue
from threading import Thread
import math


def task(table_name, cnxn_string, df_queue, columns, ignore_truncation):
    column_specification = str(tuple(columns['SELECT_SAFE_COLUMN_NAME'])).replace("'", "")
    column_select_list = column_specification.replace('(', '').replace(')', '')

    cnxn = pyodbc.connect(cnxn_string)
    crsr = cnxn.cursor()

    if ignore_truncation:
        crsr.execute('SET ANSI_WARNINGS OFF')

    crsr.execute(f'SELECT {column_select_list} INTO #TEMP FROM {table_name} WHERE 1=0')
    crsr.commit()

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
                elif data_type in ('varchar', 'date', 'datetime2', 'datetime', 'char', 'text'):
                    insert_line += f"'{row[column]}', "
                # Unicode
                elif data_type in ('nvarchar', 'nchar', 'ntext'):
                    insert_line += f"N'{row[column]}', "
                # Numeric
                elif data_type in ('bigint', 'bit', 'decimal', 'int', 'money', 'numeric', 'smallint', 'smallmoney',
                                   'tinyint', 'float', 'real'):
                    insert_line += row[column] + ', '
            insert_statement += insert_line[:-2] + '),\n'
        insert_statement = insert_statement[:-2]
        crsr.execute(insert_statement)
        crsr.commit()

    crsr.execute(f"""INSERT INTO {table_name} {column_specification}
                 SELECT {column_select_list} FROM #TEMP
                 DROP TABLE #TEMP""")
    crsr.commit()
    crsr.close()
    cnxn.close()


def thread_manager(table_name, cnxn_string, thread_count, df_queue, columns, ignore_truncation):
    threads = [Thread(target=task, args=(table_name, cnxn_string, df_queue, columns, ignore_truncation)) for i in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def to_sql(df_in, table_name, cnxn_string, index=True, replace=False, chunk_size=1000, thread_count=5, ignore_truncation=False):
    # table columns
    trimmed_name = table_name.replace('[', '').replace(']', '')
    query = f"""SELECT *,
    COLUMNPROPERTY(object_id(TABLE_NAME), COLUMN_NAME, 'IsIdentity') AS IDENTITY_FLAG
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{trimmed_name}'
    ORDER BY ORDINAL_POSITION"""
    df_table = pd.read_sql(query, pyodbc.connect(cnxn_string))
    df_table['SELECT_SAFE_COLUMN_NAME'] = df_table['COLUMN_NAME'].apply(lambda x: '[' + str(x) + ']')
    columns = df_table[df_table['IDENTITY_FLAG'] == 0][['COLUMN_NAME', 'SELECT_SAFE_COLUMN_NAME', 'DATA_TYPE']]

    if replace:
        cnxn = pyodbc.connect(cnxn_string)
        crsr = cnxn.cursor()
        crsr.execute(f"DELETE FROM {table_name}")
        crsr.commit()
        crsr.close()
        cnxn.close()

    df_out = df_in.copy()
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

    thread_manager(table_name, cnxn_string, thread_count, df_queue, columns, ignore_truncation)
