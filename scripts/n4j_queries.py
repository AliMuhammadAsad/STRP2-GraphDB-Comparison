from neo4j import GraphDatabase as GDB
import time

URI = 'bolt://localhost:7687'
AUTH = ("neo4j", "12345678")
driver = GDB.driver(URI, auth=AUTH)

# Params for Interactive Shorts - Comment all others if running Interactive Shorts
params = {
    # Interactive Shorts
    "personId" : 8796093023215,
    "messageId" : 412317002578,
}
# Params for Interactive Updates - Comment all others if running Interactive Updates
# params = {
#     # Interactive Updates
#     # Update 1
#     "placeId": 52,
#     "personId": 43985348834100,
#     "personFirstName": "Satoru",
#     "personLastName": "Gojo",
#     "gender": "male",
#     "birthday": 335318400000,
#     "creationDate": 1612137600000,
#     "locationIP": "192.168.0.100",
#     "browserUsed": "Chrome",
#     "languages": "jp:en",
#     "emails": "satorugojo@unalived.com",
#     "tagIds": [7, 8, 10],
#     "studyAt": [[2208, 1999]],
#     "workAt": [[481, 2003]],
#     # Update 2
#     "postId": 1030792154060,
#     "creationDate": 1612137600000,
#     # Update 3
#     "commentId": 412316860443,
#     "creationDate3": 1294205462006,
#     # Update 4
#     "moderatorPersonId": 933,
#     "forumId": 2099511644587,
#     "forumTitle": "InteractiveUpdate4Form",
#     "creationDate": 1612137600000,
#     "tagIds": [2875],
#     # Update 5
#     "joinDate5": 1612137600000,
#     # Update 6
#     "authorPersonId": 43985348834100,
#     "countryId": 52,
#     "forumId6": 2099511644587,
#     "postId6": 1030792154060,
#     "creationDate6": 1612137600000,
#     "locationIP6": "192.168.0.697",
#     "browserUsed6": "Chrome",
#     "language6": "jp:en",
#     "content6": "I am a post content.",
#     "imageFile6": "post-image.jpg",
#     "length6": 100,
#     "tagIds6": [7, 8, 10],
#     # Update 7
#     "authorPersonId7": 43985348834100,
#     "countryId7": 52,
#     "replyToPostId7": 1030792154060,
#     "replyToCommentId7": 1030792154061,
#     "commentId7": 1030792154062,
#     "creationDate7": 1612137600000,
#     "locationIP7": "192.168.0.697",
#     "browserUsed7": "Chrome",
#     "content7": "I am a comment content for interactive update 7.",
#     "length7": 100,
#     "tagIds7": [11, 12, 13],
#     # Update 8
#     "person1Id": 43985348834100,
#     "person2Id": 32985348833653,
#     "creationDate8": 1612145600000,
# }

# Params for Interactive Complex - Comment all others if running Interactive Complex
# params = {}

i_shorts = {
    "Interactive Short 1" : '''MATCH (n:Person {id: $personId })-[:isLocatedIn]->(p:Place)
RETURN n.firstName AS firstName, n.lastName AS lastName, n.birthday AS birthday, n.locationIP AS locationIP, n.browserUsed AS browserUsed, p.id AS cityId, n.gender AS gender, n.creationDate AS creationDate;''',
    "Interactive Short 2" : '''MATCH (:Person {id: $personId})<-[:hasCreator]-(message)
WITH message, message.id AS messageId, message.creationDate AS messageCreationDate
ORDER BY messageCreationDate DESC, messageId ASC LIMIT 10
MATCH (message)-[:replyOf*0..]->(post:Post), (post)-[:hasCreator]->(person)
RETURN messageId, coalesce(message.imageFile,message.content) AS messageContent, messageCreationDate, post.id AS postId, person.id AS personId, person.firstName AS personFirstName, person.lastName AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC''',
    "Interactive Short 3" : '''MATCH (n:Person {id: $personId })-[r:knows]-(friend)
RETURN friend.id AS personId, friend.firstName AS firstName, friend.lastName AS lastName, r.creationDate AS friendshipCreationDate
ORDER BY friendshipCreationDate DESC, toInteger(personId) ASC''',
    "Interactive Short 4" : '''MATCH (m:Message {id:  $messageId })
RETURN m.creationDate as messageCreationDate, coalesce(m.content, m.imageFile) as messageContent''',
    "Interactive Short 5" : '''MATCH (m:Message {id:  $messageId })-[:hasCreator]->(p:Person)
RETURN p.id AS personId, p.firstName AS firstName, p.lastName AS lastName''',
    "Interactive Short 6" : '''MATCH (m:Message {id: $messageId })-[:replyOf*0..]->(p:Post)<-[:containerOf]-(f:Forum)-[:hasModerator]->(mod:Person)
RETURN f.id AS forumId, f.title AS forumTitle, mod.id AS moderatorId, mod.firstName AS moderatorFirstName, mod.lastName AS moderatorLastName''',
    "Interactive Short 7" : '''MATCH (m:Message {id: $messageId })<-[:replyOf]-(c:Comment)-[:hasCreator]->(p:Person)
    OPTIONAL MATCH (m)-[:hasCreator]->(a:Person)-[r:knows]-(p)
    RETURN c.id AS commentId, c.content AS commentContent, c.creationDate AS commentCreationDate, p.id AS replyAuthorId, p.firstName AS replyAuthorFirstName, p.lastName AS replyAuthorLastName,
        CASE r WHEN null THEN false ELSE true
        END AS replyAuthorKnowsOriginalMessageAuthor
    ORDER BY commentCreationDate DESC, replyAuthorId'''
}

i_updates = {
    "Interactive Update 1" : '''MATCH (c:Place {id: $placeId})
CREATE (p:Person {
    id: $personId,
    firstName: $personFirstName,
    lastName: $personLastName,
    gender: $gender,
    birthday: $birthday,
    creationDate: $creationDate,
    locationIP: $locationIP,
    browserUsed: $browserUsed,
    languages: $languages,
    email: $emails
  })-[:isLocatedIn]->(c)
WITH p, count(*) AS dummy1
UNWIND $tagIds AS tagId
  MATCH (t:Tag {id: tagId})
  CREATE (p)-[:hasInterest]->(t)
WITH p, count(*) AS dummy2
UNWIND $studyAt AS s
  MATCH (u:Organisation {id: s[0]})
  CREATE (p)-[:studyAt {classYear: s[1]}]->(u)
WITH p, count(*) AS dummy3
UNWIND $workAt AS w
  MATCH (comp:Organisation {id: w[0]})
  CREATE (p)-[:workAt {workFrom: w[1]}]->(comp);''',
    "Interactive Update 2" : '''MATCH (person:Person {id: $personId}), (post:Post {id: $postId})
CREATE (person)-[:likes {creationDate: $creationDate}]->(post);''',
    "Interactive Update 3" : '''MATCH (person:Person {id: $personId}), (comment:Comment {id: $commentId})
CREATE (person)-[:likes {creationDate: $creationDate3}]->(comment);''',
    "Interactive Update 4" : '''MATCH (p:Person {id: $moderatorPersonId})
CREATE (f:Forum {id: $forumId, title: $forumTitle, creationDate: $creationDate})-[:hasModerator]->(p)
WITH f
UNWIND $tagIds AS tagId
  MATCH (t:Tag {id: tagId})
  CREATE (f)-[:hasTag]->(t);''',
    "Interactive Update 5" : '''MATCH (f:Forum {id: $forumId}), (p:Person {id: $personId})
CREATE (f)-[:hasMember {creationDate: $joinDate5}]->(p);''',
    "Interactive Update 6" : '''MATCH (author:Person {id: $authorPersonId}), (country:Place {id: $countryId}), (forum:Forum {id: $forumId6})
CREATE (author)<-[:hasCreator]-(p:Post:Message {
    id: $postId6,
    creationDate: $creationDate6,
    locationIP: $locationIP6,
    browserUsed: $browserUsed6,
    language: $language6,
    content: CASE $content6 WHEN '' THEN NULL ELSE $content6 END,
    imageFile: CASE $imageFile6 WHEN '' THEN NULL ELSE $imageFile6 END,
    length: $length6
  })<-[:containerOf]-(forum), (p)-[:isLocatedIn]->(country)
WITH p
UNWIND $tagIds6 AS tagId
  MATCH (t:Tag {id: tagId})
  CREATE (p)-[:hasTag]->(t);''',
    "Interactive Update 7" : '''MATCH
  (author:Person {id: $authorPersonId7}),
  (country:Place {id: $countryId7}),
  (message:Message {id: $replyToPostId7 + $replyToCommentId7 + 1}) // $replyToCommentId is -1 if the message is a reply to a post and vica versa (see spec)
CREATE (author)<-[:hasCreator]-(c:Comment:Message {
    id: $commentId7,
    creationDate: $creationDate7,
    locationIP: $locationIP7,
    browserUsed: $browserUsed7,
    content: $content7,
    length: $length7
  })-[:replyOf]->(message),
  (c)-[:isLocatedIn]->(country)
WITH c
UNWIND $tagIds7 AS tagId
  MATCH (t:Tag {id: tagId})
  CREATE (c)-[:hasTag]->(t)''',
    "Interactive Update 8" : '''MATCH (p1:Person {id: $person1Id}), (p2:Person {id: $person2Id})
CREATE (p1)-[:knows {creationDate: $creationDate}]->(p2)''',
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
# print("#---------------- Benchmarking Queries: Interactive Update -----------------#")
# print("#---------------Benchmarking Queries: Interactive Complex-----------------#")
print("#---------------- Benchmarking Queries: Interactive Shorts -----------------#")
print("#---------------------------------------------------------------------------#")

with driver.session() as session:
    # for quer, q in i_updates.items():
    # for quer, q in i_complex.items():
    for quer, q in i_shorts.items():
        start_time = time.time()
        session.run(q, params)
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

