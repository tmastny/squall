import duckdb
from typing import Iterator, Tuple

def generate_table() -> Iterator[Tuple[int, str]]:
    # This function yields tuples that will become rows in the table
    yield (1, "one")
    yield (2, "two")
    yield (3, "three")

conn = duckdb.connect("test.db")

# Create a table-returning function
conn.create_function(
    "my_table_func",      # function name
    generate_table,       # python function
    [],                   # no input parameters
    "TABLE(id INTEGER, name VARCHAR)"  # return type as table
)

# Use it in a query like a table
result = conn.sql("SELECT * FROM my_table_func()")
print(result.fetchall())
