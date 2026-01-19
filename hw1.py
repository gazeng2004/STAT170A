import pandas as pd
import numpy as np
import mysql.connector

csvfiles = {"C:/Users/Pc/Desktop/170A Homework 1 Data/acm-fellows.csv": "ACM_Fellow",
            "C:/Users/Pc/Desktop/170A Homework 1 Data/conference_ranking.csv": "Conference_Ranking",
            "C:/Users/Pc/Desktop/170A Homework 1 Data/country-info.csv": "County_info",
            "C:/Users/Pc/Desktop/170A Homework 1 Data/csrankings.csv": "CSrankings",
            "C:/Users/Pc/Desktop/170A Homework 1 Data/data.csv": "Data",
            "C:/Users/Pc/Desktop/170A Homework 1 Data/dblp-aliases.csv": "DBLP_Aliases",
            "C:/Users/Pc/Desktop/170A Homework 1 Data/field_conference.csv": "Field_Conferences",
            "C:/Users/Pc/Desktop/170A Homework 1 Data/generated-author-info.csv": "Generated_Author",
            "C:/Users/Pc/Desktop/170A Homework 1 Data/geolocation.csv": "Geolocation",
            "C:/Users/Pc/Desktop/170A Homework 1 Data/turing.csv": "Turing"
            }

edge_case = {"institution.region",
             "searchData.engineeringRepScore.rawValue",
             "searchData.businessRepScore.rawValue"}

db_host = "localhost"
db_user = "<user>"
db_password = "<<PASSWORD>>"
database = "Stat170A"


def split_name_column(chunk, name_column='name'):
    name_parts = chunk[name_column].str.split()

    chunk['first_name'] = name_parts.str[0]
    chunk['last_name'] = name_parts.str[-1]
    chunk['middle_name'] = name_parts.apply(
        lambda x: ' '.join(x[1:-1]) if len(x) > 2 else None
    )
    chunk = chunk.drop(columns=[name_column])

    cols = chunk.columns
    chunk = chunk[list(cols[-3:]) + list(cols[:-3])]

    return chunk

def process_file(csv_files, columns, chunk_size=1000) -> None:
    conn = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=database,
        charset='utf8mb4'
    )
    cursor = conn.cursor()
    columns_str = ', '.join([f'`{col}`' for col in columns])
    placeholders = ', '.join(['%s'] * len(columns))
    print(columns_str)

    insert_sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
    print(insert_sql)
    for i, chunk in enumerate(pd.read_csv(csv_files, chunksize=chunk_size)):
        chunk = chunk.replace({float('nan'): None})
        chunk = chunk.replace({np.nan: None})
        if table_name in ("CSrankings", "Generated_Author"):
            chunk = split_name_column(chunk, name_column='name')
        data = [tuple(row) for row in chunk.values]
        if i == 0:
            print(data[0])
        cursor.executemany(insert_sql, data)
        conn.commit()

    cursor.close()
    conn.close()

def get_sql_types(dtype, col) -> str:
    if col in edge_case:
        return "TEXT"

    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    else:
        return "TEXT"

def table_creater(csv_files, table_name) -> [str]:
    df = pd.read_csv(csv_files, nrows=1)
    print(df.columns)
    print(df.dtypes)

    columns = []
    col_names = []
    for col_name, dtype in df.dtypes.items():
        if table_name in ("CSrankings", "Generated_Author") and col_name == "name":
            columns.append("`first_name` TEXT")
            columns.append("`middle_name` TEXT")
            columns.append("`last_name` TEXT")
            col_names.append("first_name")
            col_names.append("middle_name")
            col_names.append("last_name")
        else:
            sql_type = get_sql_types(dtype, col_name)
            columns.append(f"`{col_name}` {sql_type}")
            col_names.append(col_name)

    create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns)})"
    print(create_table_sql)

    conn = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=database
    )
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

    print("Table Created: ", table_name)
    return col_names

if __name__ == '__main__':
    for files, table_name in csvfiles.items():
        columns = table_creater(files, table_name)
        process_file(files, columns)
