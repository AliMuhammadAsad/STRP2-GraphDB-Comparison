from neo4j import GraphDatabase
import time

# Define correct URI and AUTH arguments (no AUTH by default)
URI = "bolt://localhost:7687"
AUTH = ("", "")

queries = {
    "Load Organisation": '''
    LOAD CSV FROM '/usr/lib/memgraph/organisation_0_0.csv' WITH HEADER DELIMITER '|' AS row
    CREATE (:Organisation {id: toInteger(row.id), type: row.type, name: row.name, url: row.url});
    ''',
    "Load Place": '''
    LOAD CSV FROM '/usr/lib/memgraph/place_0_0.csv' WITH HEADER DELIMITER '|' AS row
    CREATE (:Place {id: toInteger(row.id), name: row.name, url: row.url, type: row.type});
    ''',
    "Load TagClass": '''
    LOAD CSV FROM '/usr/lib/memgraph/tagclass_0_0.csv' WITH HEADER DELIMITER '|' AS row
    CREATE (:TagClass {id: toInteger(row.id), name: row.name, url: row.url});
    ''',
    "Load Tag": '''
    LOAD CSV FROM '/usr/lib/memgraph/tag_0_0.csv' WITH HEADER DELIMITER '|' AS row
    CREATE (:Tag {id: toInteger(row.id), name: row.name, url: row.url});
    ''',
    "Organisation Located In Place": '''
    LOAD CSV FROM '/usr/lib/memgraph/organisation_isLocatedIn_place_0_0.csv' WITH HEADER DELIMITER '|' AS row
    MATCH (o:Organisation {id: toInteger(row.`Organisation.id`)}), (p:Place {id: toInteger(row.`Place.id`)})
    CREATE (o)-[:isLocatedIn]->(p);
    ''',
    "Place is Part of Place": '''
    LOAD CSV FROM '/usr/lib/memgraph/place_isPartOf_place_0_0.csv' WITH HEADER DELIMITER '|' AS row
    MATCH (p1:Place {id: toInteger(row.`Place1.id`)}), (p2:Place {id: toInteger(row.`Place2.id`)})
    CREATE (p1)-[:isPartOf]->(p2);
    ''',
    "Tag has Type TagClass": '''
    LOAD CSV FROM '/usr/lib/memgraph/tag_hasType_tagclass_0_0.csv' WITH HEADER DELIMITER '|' AS row
    MATCH (t:Tag {id: toInteger(row.`Tag.id`)}), (tc:TagClass {id: toInteger(row.`TagClass.id`)})
    CREATE (t)-[:hasType]->(tc);
    ''',
    "TagClass is SubClass of TagClass": '''
    LOAD CSV FROM '/usr/lib/memgraph/tagclass_isSubclassOf_tagclass_0_0.csv' WITH HEADER DELIMITER '|' AS row
    MATCH (tc1:TagClass {id: toInteger(row.`TagClass1.id`)}), (tc2:TagClass {id: toInteger(row.`TagClass2.id`)})
    CREATE (tc1)-[:isSubclassOf]->(tc2);
    ''',
    "Load Person": '''
    LOAD CSV FROM '/usr/lib/memgraph/person_0_0.csv' WITH HEADER DELIMITER '|' AS row
    CREATE (:Person {id: toInteger(row.id), firstName: row.firstName, lastName: row.lastName, gender: row.gender, birthday: row.birthday, creationDate: row.creationDate, locationIP: row.locationIP, browserUsed: row.browserUsed, language: row.language, email: row.email});
    ''',
    "Load Forum": '''
    LOAD CSV FROM '/usr/lib/memgraph/forum_0_0.csv' WITH HEADER DELIMITER '|' AS row
    CREATE (:Forum {id: toInteger(row.id), title: row.title, creationDate: row.creationDate});
    ''',
    "Load Post": '''
    LOAD CSV FROM '/usr/lib/memgraph/post_0_0.csv' WITH HEADER DELIMITER '|' AS row
    CREATE (:Post {id: toInteger(row.id), imageFile: case row.imageFile when '' then null else row.imageFile end, creationDate: row.creationDate, locationIP: row.locationIP, browserUsed: row.browserUsed, language: row.language, content: row.content, length: toInteger(row.length)});
    ''',
    "Load Comment": '''
    LOAD CSV FROM '/usr/lib/memgraph/comment_0_0.csv' WITH HEADER DELIMITER '|' AS row
    CREATE (:Comment {id: toInteger(row.id), creationDate: row.creationDate, locationIP: row.locationIP, browserUsed: row.browserUsed, content: row.content, length: toInteger(row.length)});
    ''',
    "Forum has Member Person": '''
    LOAD CSV FROM '/usr/lib/memgraph/forum_hasMember_person_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (f:Forum {id: toInteger(row.`Forum.id`)}), (p:Person {id: toInteger(row.`Person.id`)})
    CREATE (f)-[:hasMember {creationDate: row.joinDate}]->(p);
    ''',
    "Forum has Moderator Person": '''
    LOAD CSV FROM '/usr/lib/memgraph/forum_hasModerator_person_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (f:Forum {id: toInteger(row.`Forum.id`)}), (p:Person {id: toInteger(row.`Person.id`)})
    CREATE (f)-[:hasModerator]->(p);
    ''',
    "Forum has Tag": '''
    LOAD CSV FROM '/usr/lib/memgraph/forum_hasTag_tag_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (f:Forum {id: toInteger(row.`Forum.id`)}), (t:Tag {id: toInteger(row.`Tag.id`)})
    CREATE (f)-[:hasTag]->(t);
    ''',
    "Forum container of Post": '''
    LOAD CSV FROM '/usr/lib/memgraph/forum_containerOf_post_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (f:Forum {id: toInteger(row.`Forum.id`)}), (p:Post {id: toInteger(row.`Post.id`)})
    CREATE (f)-[:containerOf]->(p);
    ''',
    "Post has Tag": '''
    LOAD CSV FROM '/usr/lib/memgraph/post_hasTag_tag_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (p:Post {id: toInteger(row.`Post.id`)}), (t:Tag {id: toInteger(row.`Tag.id`)})
    CREATE (p)-[:hasTag]->(t);
    ''',
    "Post is located in Place": '''
    LOAD CSV FROM '/usr/lib/memgraph/post_isLocatedIn_place_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (p:Post {id: toInteger(row.`Post.id`)}), (pl:Place {id: toInteger(row.`Place.id`)})
    CREATE (p)-[:isLocatedIn]->(pl);
    ''',
    "Post has Creator Person": '''
    LOAD CSV FROM '/usr/lib/memgraph/post_hasCreator_person_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (p:Post {id: toInteger(row.`Post.id`)}), (pe:Person {id: toInteger(row.`Person.id`)})
    CREATE (p)-[:hasCreator]->(pe);
    ''',
    "Comment has Tag": '''
    LOAD CSV FROM '/usr/lib/memgraph/comment_hasTag_tag_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (c:Comment {id: toInteger(row.`Comment.id`)}), (t:Tag {id: toInteger(row.`Tag.id`)})
    CREATE (c)-[:hasTag]->(t);
    ''',
    "Comment is located in Place": '''
    LOAD CSV FROM '/usr/lib/memgraph/comment_isLocatedIn_place_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (c:Comment {id: toInteger(row.`Comment.id`)}), (p:Place {id: toInteger(row.`Place.id`)})
    CREATE (c)-[:isLocatedIn]->(p);
    ''',
    "Comment is Reply of Comment": '''
    LOAD CSV FROM '/usr/lib/memgraph/comment_replyOf_comment_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (c1:Comment {id: toInteger(row.`Comment1.id`)}), (c2:Comment {id: toInteger(row.`Comment2.id`)})
    CREATE (c1)-[:replyOf]->(c2);
    ''',
    "Comment is Reply of Post": '''
    LOAD CSV FROM '/usr/lib/memgraph/comment_replyOf_post_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (c:Comment {id: toInteger(row.`Comment.id`)}), (p:Post {id: toInteger(row.`Post.id`)})
    CREATE (c)-[:replyOfPost]->(p);
    ''',
    "Comment has Creator Person": '''
    LOAD CSV FROM '/usr/lib/memgraph/comment_hasCreator_person_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (c:Comment {id: toInteger(row.`Comment.id`)}), (p:Person {id: toInteger(row.`Person.id`)})
    CREATE (c)-[:hasCreator]->(p);
    ''',
    "Person Likes (Post)": '''
    LOAD CSV FROM '/usr/lib/memgraph/person_likes_post_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (p:Person {id: toInteger(row.`Person.id`)}), (post:Post {id: toInteger(row.`Post.id`)})
    CREATE (p)-[:likes {creationDate: row.creationDate}]->(post);
    ''',
    "Person Likes (Comment)": '''
    LOAD CSV FROM '/usr/lib/memgraph/person_likes_comment_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (p:Person {id: toInteger(row.`Person.id`)}), (c:Comment {id: toInteger(row.`Comment.id`)})
    CREATE (p)-[:likes {creationDate: row.creationDate}]->(c);
    ''',
    "Person Knows Person": '''
    LOAD CSV FROM '/usr/lib/memgraph/person_knows_person_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (p1:Person {id: toInteger(row.`Person1.id`)}), (p2:Person {id: toInteger(row.`Person2.id`)})
    CREATE (p1)-[:knows {creationDate: row.creationDate}]->(p2);
    ''',
    "Person has interest Tag": '''
    LOAD CSV FROM '/usr/lib/memgraph/person_hasInterest_tag_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (p:Person {id: toInteger(row.`Person.id`)}), (t:Tag {id: toInteger(row.`Tag.id`)})
    CREATE (p)-[:hasInterest]->(t);
    ''',
    "Person study at University": '''
    LOAD CSV FROM '/usr/lib/memgraph/person_studyAt_organisation_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (p:Person {id: toInteger(row.`Person.id`)}), (o:Organisation {id: toInteger(row.`Organisation.id`)})
    CREATE (p)-[:studyAt {classYear: toInteger(row.classYear)}]->(o);
    ''',
    "Person works at Company": '''
    LOAD CSV FROM '/usr/lib/memgraph/person_workAt_organisation_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (p:Person {id: toInteger(row.`Person.id`)}), (o:Organisation {id: toInteger(row.`Organisation.id`)})
    CREATE (p)-[:workAt {workFrom: toInteger(row.workFrom)}]->(o);
    ''',
    "Person is Located in Place": '''
    LOAD CSV FROM '/usr/lib/memgraph/person_isLocatedIn_place_0_0.csv' WITH HEADER DELIMITER '|' as row
    MATCH (p:Person {id: toInteger(row.`Person.id`)}), (pl:Place {id: toInteger(row.`Place.id`)})
    CREATE (p)-[:isLocatedIn]->(pl);
    '''
}


total_time=0

with GraphDatabase.driver(URI, auth=AUTH) as client:
    # Check the connection
    client.verify_connectivity()


    for query_name, query in queries.items():
        # Measure the time for each query
        start_time = time.time()
        try:
            # Execute the query
            client.execute_query(query)
            # Calculate execution time
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"Query '{query_name}' Execution Time: {execution_time:.4f} seconds")
            total_time += execution_time

        except Exception as e:
            print(f"Error executing query '{query_name}': {e}")
            print("Aborting and clearing the graph...")
            client.execute_query("MATCH (n) DETACH DELETE n")
            break


    client.close()
    print("Total time taken", total_time)
