import splink.comparison_library as cl
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
import mysql.connector
import pandas as pd
from hw1 import split_name_column

db_host = "localhost"
db_user = "<root>"
db_password = "<Password>"
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
            #block_on("first_name", "last_name"),
            #block_on("last_name", "middle_name"),
            #block_on("last_name")
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
    query = "CREATE TABLE IF NOT EXISTS " + "clean_csrankings"
    cursor = conn.cursor()


if __name__ == "__main__":
    sql_df = sql_pandas_get()
    sql_df = edit_pandas_num(sql_df)
    matches = splink_names(sql_df)
    matches = matches.sort_values(by=["cluster_id", "id"])
    print(matches)
    matches.to_csv("splink_matches.csv", index=False)
    duplicates = matches[matches.duplicated(subset='cluster_id', keep=False)]
    duplicates = duplicates.sort_values('cluster_id')

    print(f"\nFound {len(duplicates)} duplicate rows across {duplicates['cluster_id'].nunique()} clusters")

    # Show the duplicates
    print("\nDuplicate records:")
    print(duplicates[['cluster_id', 'id', 'scholarid', 'affiliation',
                      'clean_author_name']])

    # Save duplicates to separate CSV for review
    duplicates.to_csv("duplicate_scholars_only.csv", index=False)
    print(f"\nSaved duplicates to duplicate_scholars_only.csv")
