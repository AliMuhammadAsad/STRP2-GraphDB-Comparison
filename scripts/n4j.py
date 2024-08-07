from neo4j import GraphDatabase as GDB
import time

URI = 'bolt://localhost:7687'
AUTH = ("neo4j", "12345678")
driver = GDB.driver(URI, auth=AUTH)

def delete_all(tx):
    tx.run('''call apoc.periodic.iterate('call apoc.periodic.iterate('match (n) return n', 'detach delete n', {batchSize:1000, iterateList:true})''')
    print("Deleted all nodes")
    
def run_query(tx, query):
    tx.run(query)

load_static_nodes = [
    '''// Load Organisation
call apoc.periodic.iterate(
'load csv with headers from "file:///static/organisation_0_0.csv" as row fieldterminator "|" return row',
'create (o:Organisation {id:toInteger(row.id), type:row.type, name:row.name, url:row.url})',
{batchSize:1000, iterateList:true}
);''',
'''// Load Place
call apoc.periodic.iterate(
'load csv with headers from "file:///static/place_0_0.csv" as row fieldterminator "|" return row',
'create (p:Place {id: toInteger(row.id), name: row.name, url: row.url, type: row.type})',
{batchSize:1000, iterateList:true}
);''',
'''// Load TagClass
call apoc.periodic.iterate(
'load csv with headers from "file:///static/tagclass_0_0.csv" AS row fieldterminator "|" return row',
'create (tc:TagClass {id: toInteger(row.id), name: row.name, url: row.url})',
{batchSize:1000, iterateList:true}
);''',
'''// Load Tag
call apoc.periodic.iterate(
'load csv with headers from "file:///static/tag_0_0.csv" AS row fieldterminator "|" return row',
'create (t:Tag {id: toInteger(row.id), name: row.name, url: row.url})',
{batchSize:1000, iterateList:true}
);'''
]
load_static_relationships = ['''
// Organisation Located In Place
call apoc.periodic.iterate(
'load csv with headers from "file:///static///organisation_isLocatedIn_place_0_0.csv" as row fieldterminator "|" return row',
'match (o:Organisation {id: toInteger(row.`Organisation.id`)}), (p:Place {id: toInteger(row.`Place.id`)})
create (o)-[:isLocatedIn]->(p)',
{batchSize:1000, iterateList:true}
);''',
'''// Place is part of Place
call apoc.periodic.iterate(
'load csv with headers from "file:///static/place_isPartOf_place_0_0.csv" as row fieldterminator "|" return row',
'match (p1:Place {id: toInteger(row.`Place1.id`)}), (p2:Place {id: toInteger(row.`Place2.id`)})
create (p1)-[:isPartOf]->(p2)',
{batchSize:1000, iterateList:true}
);''',
'''// Tag has type TagClass
call apoc.periodic.iterate(
'load csv with headers from "file:///static/tag_hasType_tagclass_0_0.csv" as row fieldterminator "|" return row',
'match (t:Tag {id: toInteger(row.`Tag.id`)}), (tc:TagClass {id: toInteger(row.`TagClass.id`)})
create (t)-[:hasType]->(tc)',
{batchSize:1000, iterateList:true}
);''',
'''// TagClass is Subclass of TagClass
call apoc.periodic.iterate(
'load csv with headers from "file:///static/tagclass_isSubclassOf_tagclass_0_0.csv" as row fieldterminator "|" return row',
'match (tc1:TagClass {id: toInteger(row.`TagClass1.id`)}), (tc2:TagClass {id: toInteger(row.`TagClass2.id`)})
create (tc1)-[:isSubclassOf]->(tc2)',
{batchSize:1000, iterateList:true}
);'''
]

load_dynamic_nodes = ['''
// Load Person
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/person_0_0.csv" as row fieldterminator "|" return row',
  'create (p:Person {id: toInteger(row.id), firstName: row.firstName, lastName: row.lastName, gender: row.gender, birthday: row.birthday, creationDate: row.creationDate, locationIP: row.locationIP, browserUsed: row.browserUsed, language: row.language, email: row.email})',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Forum
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/forum_0_0.csv" as row fieldterminator "|" return row',
  'create (f:Forum {id: toInteger(row.id), title: row.title, creationDate: row.creationDate})',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Post
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/post_0_0.csv" as row fieldterminator "|" return row',
  'create (p:Post {id: toInteger(row.id), imageFile: case row.imageFile when "" then null else row.imageFile end, creationDate: row.creationDate, locationIP: row.locationIP, browserUsed: row.browserUsed, language: row.language, content: row.content, length: toInteger(row.length)})',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Comment
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/comment_0_0.csv" as row fieldterminator "|" return row',
  'create (c:Comment {id: toInteger(row.id), creationDate: row.creationDate, locationIP: row.locationIP, browserUsed: row.browserUsed, content: row.content, length: toInteger(row.length)})',
  {batchSize: 1000, iterateList: true}
);''']
load_dynamic_relationships = ['''
// Load Forum has member Person
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/forum_hasMember_person_0_0.csv" as row fieldterminator "|" return row',
  'match (f:Forum {id: toInteger(row.`Forum.id`)}), (p:Person {id: toInteger(row.`Person.id`)}) create (f)-[:hasMember {creationDate: row.joinDate}]->(p)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load forum has moderator person
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/forum_hasModerator_person_0_0.csv" as row fieldterminator "|" return row',
  'match (f:Forum {id: toInteger(row.`Forum.id`)}), (p:Person {id: toInteger(row.`Person.id`)}) create (f)-[:hasModerator]->(p)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Forum has Tag
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/forum_hasTag_tag_0_0.csv" as row fieldterminator "|" return row',
  'match (f:Forum {id: toInteger(row.`Forum.id`)}), (t:Tag {id: toInteger(row.`Tag.id`)}) create (f)-[:hasTag]->(t)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Forum container of Post
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/forum_containerOf_post_0_0.csv" as row fieldterminator "|" return row',
  'match (f:Forum {id: toInteger(row.`Forum.id`)}), (p:Post {id: toInteger(row.`Post.id`)}) create (f)-[:containerOf]->(p)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Post has Tag
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/post_hasTag_tag_0_0.csv" as row fieldterminator "|" return row',
  'match (p:Post {id: toInteger(row.`Post.id`)}), (t:Tag {id: toInteger(row.`Tag.id`)}) create (p)-[:hasTag]->(t)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Post is located in Place
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/post_isLocatedIn_place_0_0.csv" as row fieldterminator "|" return row',
  'match (p:Post {id: toInteger(row.`Post.id`)}), (pl:Place {id: toInteger(row.`Place.id`)}) create (p)-[:isLocatedIn]->(pl)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Post has Creator Person
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/post_hasCreator_person_0_0.csv" as row fieldterminator "|" return row',
  'match (p:Post {id: toInteger(row.`Post.id`)}), (pe:Person {id: toInteger(row.`Person.id`)}) create (p)-[:hasCreator]->(pe)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Comment has Tag
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/comment_hasTag_tag_0_0.csv" as row fieldterminator "|" return row',
  'match (c:Comment {id: toInteger(row.`Comment.id`)}), (t:Tag {id: toInteger(row.`Tag.id`)}) create (c)-[:hasTag]->(t)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Comment is located in Place
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/comment_isLocatedIn_place_0_0.csv" as row fieldterminator "|" return row',
  'match (c:Comment {id: toInteger(row.`Comment.id`)}), (p:Place {id: toInteger(row.`Place.id`)}) create (c)-[:isLocatedIn]->(p)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Comment is reply of Comment
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/comment_replyOf_comment_0_0.csv" as row fieldterminator "|" return row',
  'match (c1:Comment {id: toInteger(row.`Comment1.id`)}), (c2:Comment {id: toInteger(row.`Comment2.id`)}) create (c1)-[:replyOf]->(c2)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Comment is reply of Post
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/comment_replyOf_post_0_0.csv" as row fieldterminator "|" return row',
  'match (c:Comment {id: toInteger(row.`Comment.id`)}), (p:Post {id: toInteger(row.`Post.id`)}) create (c)-[:replyOfPost]->(p)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Comment has Creator Person
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/comment_hasCreator_person_0_0.csv" as row fieldterminator "|" return row',
  'match (c:Comment {id: toInteger(row.`Comment.id`)}), (p:Person {id: toInteger(row.`Person.id`)}) create (c)-[:hasCreator]->(p)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Person Likes (Post)
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/person_likes_post_0_0.csv" as row fieldterminator "|" return row',
  'match (p:Person {id: toInteger(row.`Person.id`)}), (post:Post {id: toInteger(row.`Post.id`)}) create (p)-[:likes {creationDate: row.creationDate}]->(post)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Person Likes (Comment)
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/person_likes_comment_0_0.csv" as row fieldterminator "|" return row',
  'match (p:Person {id: toInteger(row.`Person.id`)}), (c:Comment {id: toInteger(row.`Comment.id`)}) create (p)-[:likes {creationDate: row.creationDate}]->(c)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Person Knows Person
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/person_knows_person_0_0.csv" as row fieldterminator "|" return row',
  'match (p1:Person {id: toInteger(row.`Person1.id`)}), (p2:Person {id: toInteger(row.`Person2.id`)}) create (p1)-[:knows {creationDate: row.creationDate}]->(p2)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Person has Interest Tag
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/person_hasInterest_tag_0_0.csv" as row fieldterminator "|" return row',
  'match (p:Person {id: toInteger(row.`Person.id`)}), (t:Tag {id: toInteger(row.`Tag.id`)}) create (p)-[:hasInterest]->(t)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Person study at University
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/person_studyAt_organisation_0_0.csv" as row fieldterminator "|" return row',
  'match (p:Person {id: toInteger(row.`Person.id`)}), (o:Organisation {id: toInteger(row.`Organisation.id`)}) create (p)-[:studyAt {classYear: toInteger(row.classYear)}]->(o)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Load Person Company
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/person_workAt_organisation_0_0.csv" as row fieldterminator "|" return row',
  'match (p:Person {id: toInteger(row.`Person.id`)}), (o:Organisation {id: toInteger(row.`Organisation.id`)}) create (p)-[:workAt {workFrom: toInteger(row.workFrom)}]->(o)',
  {batchSize: 1000, iterateList: true}
);''',
'''// Person is Located In Place
call apoc.periodic.iterate(
  'load csv with headers from "file:///dynamic/person_isLocatedIn_place_0_0.csv" as row fieldterminator "|" return row',
  'match (p:Person {id: toInteger(row.`Person.id`)}), (pl:Place {id: toInteger(row.`Place.id`)}) create (p)-[:isLocatedIn]->(pl)',
  {batchSize: 1000, iterateList: true}
);''']

with driver.session() as session:
    # Timing Loading Complete Dataset using APOC
    start_time = time.time()
    for q in load_static_nodes: session.run(q)
    for q in load_static_relationships: session.run(q)
    for q in load_dynamic_nodes: session.run(q)
    for q in load_dynamic_relationships: session.run(q)
    end_time = time.time()

# Printing time in milliseconds:
print(f"Time taken to load dataset: {(end_time - start_time) * 1000:.2f} ms")
# Printing time in seconds:
print(f"Time taken to load dataset: {end_time - start_time:.2f} s")
# Printing time in hours:
print(f"Time taken to load dataset: {(end_time - start_time) / 3600:.2f} h")


driver.close()
session.close()
