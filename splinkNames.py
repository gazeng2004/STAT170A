import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
import mysql.connector
import pandas as pd
from hw1 import split_name_column

db_host = "localhost"
db_user = "<user>"
db_password = "<password>"
database = "Stat170A"
db_table = "csrankings"
unique_id = "scholarid"

def edit_pandas_num(df: pd.DataFrame) -> pd.DataFrame:
    mask = df["last_name"].astype(str).str.isnumeric()
    cleaned_df = df[mask]
    split_df = split_name_column(cleaned_df, "clean_author_name", False)
    df.loc[mask, split_df.columns] = split_df
    return df

def sql_pandas_get() -> pd.DataFrame:
    conn = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=database
    )

    query = "SELECT * FROM " + db_table
    df = pd.read_sql(query, conn)
    df['id'] = df.index
    #df = df.drop(columns=['first_name', 'middle_name', 'last_name'])
    conn.close()
    return df

def splink_names(df: pd.DataFrame) -> pd.DataFrame:
    settings = SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="id",
        comparisons=[
            cl.NameComparison("first_name"),
            cl.NameComparison("middle_name"),
            cl.NameComparison("last_name")
        ],
        blocking_rules_to_generate_predictions=[
            block_on("last_name", "affiliation"),
        ]
    )
    db_api = DuckDBAPI()
    linker = Linker(df, settings, db_api=db_api)
    df_predictions = linker.inference.predict(threshold_match_probability=0.2)
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        df_predictions, threshold_match_probability=0.5
    )
    return clusters.as_pandas_dataframe()

def dataframeToSQL(df: pd.DataFrame) -> None:
    conn = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=database
    )

    cols_definitions = [
        "first_name TEXT",
        "middle_name TEXT",
        "last_name TEXT",
        "affiliation VARCHAR(200)",
        "homepage TEXT",
        "scholarid VARCHAR(150)",
        "clean_author_name TEXT",
        "PRIMARY KEY (scholarid, affiliation)"
    ]
    cols_def_str = ", ".join(cols_definitions)

    cols_names = ["first_name", "middle_name", "last_name", "affiliation",
                  "homepage", "scholarid", "clean_author_name"]
    cols_names_str = ", ".join(cols_names)

    table = "Filtered_CSrankings"
    placeholders = ', '.join(['%s'] * len(cols_names))
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS `{table}` ({cols_def_str})")
    conn.commit()

    insert_query = f"INSERT IGNORE INTO `{table}` ({cols_names_str}) VALUES ({placeholders})"
    df_deduplicated = df.drop_duplicates(subset=['cluster_id', 'affiliation'], keep='first')
    data = []
    for _, row in df_deduplicated.iterrows():
        data.append(tuple(row[col] for col in cols_names))

    try:
        cursor.executemany(insert_query, data)
        conn.commit()
        print(f"Successfully inserted {len(data)} records")
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    sql_df = sql_pandas_get()
    sql_df = edit_pandas_num(sql_df)
    matches = splink_names(sql_df)
    matches = matches.sort_values(by=["cluster_id", "id"])
    dataframeToSQL(matches)

    print(matches)
    matches.to_csv("splink_matches.csv", index=False)
    duplicates = matches[matches.duplicated(subset='cluster_id', keep=False)]
    duplicates = duplicates.sort_values('cluster_id')

    print(f"\nFound {len(duplicates)} duplicate rows across {duplicates['cluster_id'].nunique()} clusters")

    print("\nDuplicate records:")
    print(duplicates[['cluster_id', 'id', 'scholarid', 'affiliation',
                      'clean_author_name']])

    duplicates.to_csv("duplicate_scholars_only.csv", index=False)
    print(f"\nSaved duplicates to duplicate_scholars_only.csv")
