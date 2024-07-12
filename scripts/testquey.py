from neo4j import GraphDatabase as GDB
import time

URI = 'bolt://localhost:7687'
AUTH = ("neo4j", "12345678")
driver = GDB.driver(URI, auth=AUTH)

def delete_all(tx):
    tx.run("MATCH (n) DETACH DELETE n")
    print("Deleted all nodes")

query = '''
load csv with headers from 'file:///test_300.csv' as row
merge (a:node {id: tointeger(row.node_1)})
merge (b:node {id: tointeger(row.node_2)})
merge (a)-[:CONNECTED_TO]->(b)
'''


with driver.session() as session:
    start_time = time.time()
    session.run(query)
    end_time = time.time()

# Printing time in milliseconds:
print(f"Time taken: {(end_time - start_time) * 1000:.2f} ms")


driver.close()
session.close()