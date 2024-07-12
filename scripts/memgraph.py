from neo4j import GraphDatabase
import time

# Define correct URI and AUTH arguments (no AUTH by default)
URI = "bolt://localhost:7687"
AUTH = ("", "")

with GraphDatabase.driver(URI, auth=AUTH) as client:
    # Check the connection
    client.verify_connectivity()

    # Measure the time for creating a user in the database
    start_time = time.time()
    records, summary, keys = client.execute_query(
        "CREATE (u:User {name: $name, password: $password}) RETURN u.name AS name;",
        name="John",
        password="pass",
        database_="memgraph",
    )
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Query Execution Time (Create User): {execution_time:.4f} seconds")

    # Get the result
    for record in records:
        print(record["name"])

    # Print the query counters
    print(summary.counters)

    # Measure the time for finding a user in the database
    start_time = time.time()
    records, summary, keys = client.execute_query(
        "MATCH (u:User {name: $name}) RETURN u.name AS name",
        name="John",
        database_="memgraph",
    )
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Query Execution Time (Find User): {execution_time:.4f} seconds")

    # Get the result
    for record in records:
        print(record["name"])

    # Print the query
    print(summary.query)
