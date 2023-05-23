# %%
import sqlite3

# DONE Get a list of all tables, and per table:
# DONE Get a list of all columns, and per column:
    # get the "% Uniqueness" value as SELECT (SELECT COUNT(DISTINCT x) FROM y) / (SELECT COUNT(*) FROM y)  
    # if the % unqieuness is 2 std. deviations below the mean, then categorical
        # if Categorical, then get a list of all distinct values
            # clean all distinct values
        # if Numerical, then get statistics
# Take a sample of the dtypes
    # if categorical & non-null, iteratively compare all columns of clean distinct values against each other
    # if time-series...?    

# depending on cardinality, analyze left, outer, and inner joins
    # what % population is able to be joined?
    # what distinct values exist/don't exist in the other population?

# %%

data_dict = {
    "table_name": "table"

# what is the data within the table?
    , "columns": {
        "column_1": {
            "distinct_values": ["values_1", "values_2", "values_3"]
            , "cln_distinct_values": ["VALUES1", "VALUES2", "VALUES3"]
        }


# what are the potential tables I can join to?
    , "joins": {
        "table_2": {
            "left_join" : {
                # what can I join the tables on?
                "column_join": "column_2"
                
                # how much of the join succeeds?
                , "population": 0.100
                }
            , "inner_join": {
                "population": 0.50
                , "column_join": "column_2"
                }
            }
        }
    }
    
}
# %%
con = sqlite3.connect("tutorial.db")
cur = con.cursor()
cur.execute("CREATE TABLE movie(title, year, score)")

# %%
data = [
    ("Monty Python Live at the Hollywood Bowl", 1982, 7.9),
    ("Monty Python's The Meaning of Life", 1983, 7.5),
    ("Monty Python's Life of Brian", 1979, 8.0),
]
# %%
cur.executemany("INSERT INTO movie VALUES(?, ?, ?)", data)

# %%
con.commit()  # Remember to commit the transaction after executing INSERT.

# %%
master_table = cur.execute("SELECT * FROM sqlite_master").fetchall()

# %%
cur.execute("PRAGMA table_info(movie)").fetchall()
# %%
"""
 "name"
, "type"
, "notnull"
, "dflt_value"
, "pk" 
"""