from neo4j import GraphDatabase as GDB
import time

URI = 'bolt://localhost:7687'
AUTH = ("neo4j", "12345678")
driver = GDB.driver(URI, auth=AUTH)

i_shorts = {
    "Interactive Short 1" : '''''',
    "Interactive Short 2" : '''''',
    "Interactive Short 3" : '''''',
    "Interactive Short 4" : '''''',
    "Interactive Short 5" : '''''',
    "Interactive Short 6" : '''''',
    "Interactive Short 7" : ''''''
}

i_updates = {
    "Interactive Update 1" : '''''',
    "Interactive Update 2" : '''''',
    "Interactive Update 3" : '''''',
    "Interactive Update 4" : '''''',
    "Interactive Update 5" : '''''',
    "Interactive Update 6" : '''''',
    "Interactive Update 7" : '''''',
    "Interactive Update 8" : '''''',
}

i_complex = {
    "Interactive Complex 1" : '''''',
    "Interactive Complex 2" : '''''',
    "Interactive Complex 3" : '''''',
    "Interactive Complex 4" : '''''',
    "Interactive Complex 5" : '''''',
    "Interactive Complex 6" : '''''',
    "Interactive Complex 7" : '''''',
    "Interactive Complex 8" : '''''',
    "Interactive Complex 9" : '''''',
    "Interactive Complex 10" : '''''',
    "Interactive Complex 11" : '''''',
    "Interactive Complex 12" : '''''',
    "Interactive Complex 13" : '''''',
    "Interactive Complex 14" : '''''',
}

total_time = 0
lowest = 10000000000000000000000; lowest_name = ""
highest = 0; highest_name = ""
query_count = 0 

print("#---------------------------------------------------------------------------#")
# print("#---------------- Benchmarking Queries: Interactive Updates -----------------#")
# print("#---------------- Benchmarking Queries: Interactive Complex -----------------#")
print("#---------------- Benchmarking Queries: Interactive Shorts -----------------#")
print("#---------------------------------------------------------------------------#")

with driver.session() as session:
    # for quer, q in i_updates.items():
    # for quer, q in i_complex.items():
    for quer, q in i_shorts.items():
        start_time = time.time()
        session.run(q)
        end_time = time.time()
        t_taken = end_time - start_time
        if t_taken < lowest: 
            lowest = t_taken; lowest_name = quer
        if t_taken > highest: 
            highest = t_taken; highest_name = quer
        print(f"Time taken to run {quer}: {t_taken * 1000:.2f} ms")
        total_time += t_taken
        query_count += 1

print("\n")
print("#---------------------------------------------------------------------------#")
print("#------------------------------ Execution Times ----------------------------#")
print("#---------------------------------------------------------------------------#")
print(f"Fastest Query: {lowest_name} with {lowest * 1000:.2f} ms")
print(f"Slowest Query: {highest_name} with {highest * 1000:.2f} ms")
print(f"Total time taken: {total_time * 1000:.2f} ms")
print(f"Average time taken: {total_time / query_count * 1000:.2f} ms")
print("#---------------------------------------------------------------------------#")

