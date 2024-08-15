from neo4j import GraphDatabase as GDB
import time

URI = 'bolt://localhost:7687'
AUTH = ("neo4j", "12345678")
driver = GDB.driver(URI, auth=AUTH)

# Params for Interactive Shorts - Comment all others if running Interactive Shorts
# params = {
#     # Interactive Shorts
#     "personId" : 8796093023215,
#     "messageId" : 412317002578,
# }
# Params for Interactive Updates - Comment all others if running Interactive Updates
params = {
    # Interactive Updates
    # Update 1
    "placeId": 52,
    "personId": 43985348834100,
    "personFirstName": "Satoru",
    "personLastName": "Gojo",
    "gender": "male",
    "birthday": 335318400000,
    "creationDate": 1612137600000,
    "locationIP": "192.168.0.100",
    "browserUsed": "Chrome",
    "languages": "jp:en",
    "emails": "satorugojo@unalived.com",
    "tagIds": [7, 8, 10],
    "studyAt": [[2208, 1999]],
    "workAt": [[481, 2003]],
    # Update 2
    "postId": 1030792154060,
    "creationDate": 1612137600000,
    # Update 3
    "commentId": 412316860443,
    "creationDate3": 1294205462006,
    # Update 4
    "moderatorPersonId": 933,
    "forumId": 2099511644587,
    "forumTitle": "InteractiveUpdate4Form",
    "creationDate": 1612137600000,
    "tagIds": [2875],
    # Update 5
    "joinDate5": 1612137600000,
    # Update 6
    "authorPersonId": 43985348834100,
    "countryId": 52,
    "forumId6": 2099511644587,
    "postId6": 1030792154060,
    "creationDate6": 1612137600000,
    "locationIP6": "192.168.0.697",
    "browserUsed6": "Chrome",
    "language6": "jp:en",
    "content6": "I am a post content.",
    "imageFile6": "post-image.jpg",
    "length6": 100,
    "tagIds6": [7, 8, 10],
    # Update 7
    "authorPersonId7": 43985348834100,
    "countryId7": 52,
    "replyToPostId7": 1030792154060,
    "replyToCommentId7": 1030792154061,
    "commentId7": 1030792154062,
    "creationDate7": 1612137600000,
    "locationIP7": "192.168.0.697",
    "browserUsed7": "Chrome",
    "content7": "I am a comment content for interactive update 7.",
    "length7": 100,
    "tagIds7": [11, 12, 13],
    # Update 8
    "person1Id": 43985348834100,
    "person2Id": 32985348833653,
    "creationDate8": 1612145600000,
}

# Params for Interactive Complex - Comment all others if running Interactive Complex
# params = {
#   # Compelex 1
#   "personId1": 8796093023522,
#   "firstName1": "Andrew",
#   # Complex 2
#   "maxDate": "1347528281121",
#   # Complex 3
#   "countryXName": "Angola",
#   "countryYName": "Colombia",
#   "startDate": "0",
#   "endDate": "1347528281121",
#   # Complex 4 - same as above
#   # Complex 5
#   "minDate": "0",
#   # Complex 6
#   "tagName": "Carl_Gustaf_Emil_Mannerheim",
#   # Complex 7 - same as above
#   # Complex 8 - same as above
#   # Complex 10
#   "month": 5,
#   # Complex 11
#   "countryName11": "Hungary",
#   "workFromYear11": 2011,
#   # Complex 12
#   "tagClassName12": "Monarch",
#   # Complex 13
#   "person1Id13": 8796093023522,
#   "person2Id13": 19791209300796,
#   # Complex 14 - same as above
# }

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
    "Interactive Complex 1" : '''MATCH (p:Person {id: $personId1}), (friend:Person {firstName: $firstName1})
       WHERE NOT p=friend
       WITH p, friend
       MATCH path = shortestPath((p)-[:knows*1..3]-(friend))
       WITH min(length(path)) AS distance, friend
ORDER BY
    distance ASC,
    friend.lastName ASC,
    toInteger(friend.id) ASC
LIMIT 20

MATCH (friend)-[:isLocatedIn]->(friendCity:Place)
OPTIONAL MATCH (friend)-[studyAt:studyAt]->(uni:Organisation)-[:isLocatedIn]->(uniCity:Place)
WITH friend, collect(
    CASE uni.name
        WHEN null THEN null
        ELSE [uni.name, studyAt.classYear, uniCity.name]
    END ) AS unis, friendCity, distance

OPTIONAL MATCH (friend)-[workAt:workAt]->(company:Organisation)-[:isLocatedIn]->(companyCountry:Place)
WITH friend, collect(
    CASE company.name
        WHEN null THEN null
        ELSE [company.name, workAt.workFrom, companyCountry.name]
    END ) AS companies, unis, friendCity, distance

RETURN
    friend.id AS friendId,
    friend.lastName AS friendLastName,
    distance AS distanceFromPerson,
    friend.birthday AS friendBirthday,
    friend.creationDate AS friendCreationDate,
    friend.gender AS friendGender,
    friend.browserUsed AS friendBrowserUsed,
    friend.locationIP AS friendLocationIp,
    friend.email AS friendEmails,
    friend.language AS friendLanguages,
    friendCity.name AS friendCityName,
    unis AS friendUniversities,
    companies AS friendCompanies
ORDER BY
    distanceFromPerson ASC,
    friendLastName ASC,
    toInteger(friendId) ASC
LIMIT 20''',
    "Interactive Complex 2" : '''MATCH (:Person {id: $personId1})-[:knows]-(friend:Person)<-[:hasCreator]-(message:Message)
    WHERE message.creationDate <= $maxDate
    RETURN
        friend.id AS personId,
        friend.firstName AS personFirstName,
        friend.lastName AS personLastName,
        message.id AS postOrCommentId,
        coalesce(message.content,message.imageFile) AS postOrCommentContent,
        message.creationDate AS postOrCommentCreationDate
    ORDER BY
        postOrCommentCreationDate DESC,
        toInteger(postOrCommentId) ASC
    LIMIT 20''',
    "Interactive Complex 3" : '''MATCH (countryX:Place {name: $countryXName }),
      (countryY:Place {name: $countryYName }),
      (person:Person {id: $personId1})
WITH person, countryX, countryY
LIMIT 1
MATCH (city:Place)-[:isPartOf]->(country:Place)
WHERE country IN [countryX, countryY]
WITH person, countryX, countryY, collect(city) AS cities
MATCH (person)-[:knows*1..2]-(friend)-[:isLocatedIn]->(city)
WHERE NOT person=friend AND NOT city IN cities
WITH DISTINCT friend, countryX, countryY
MATCH (friend)<-[:hasCreator]-(message),
      (message)-[:isLocatedIn]->(country)
WHERE $endDate > message.creationDate >= $startDate AND
      country IN [countryX, countryY]
WITH friend,
     CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX,
     CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY
WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
WHERE xCount>0 AND yCount>0
RETURN friend.id AS friendId,
       friend.firstName AS friendFirstName,
       friend.lastName AS friendLastName,
       xCount,
       yCount,
       xCount + yCount AS xyCount
ORDER BY xyCount DESC, friendId ASC
LIMIT 20''',
    "Interactive Complex 4" : '''MATCH (person:Person {id: $personId1})-[:knows]-(friend:Person),
      (friend)<-[:hasCreator]-(post:Post)-[:hasTag]->(tag)
WITH DISTINCT tag, post
WITH tag,
     CASE
       WHEN $startDate <= post.creationDate < $endDate THEN 1
       ELSE 0
     END AS valid,
     CASE
       WHEN post.creationDate < $startDate THEN 1
       ELSE 0
     END AS inValid
WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
WHERE postCount>0 AND inValidPostCount=0
RETURN tag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10''',
    "Interactive Complex 5" : '''MATCH (person:Person {id: $personId1})-[:knows*1..2]-(friend)
WHERE
    NOT person=friend
WITH DISTINCT friend
MATCH (friend)<-[membership:hasMember]-(forum)
WHERE
    membership.creationDate > $minDate
WITH
    forum,
    collect(friend) AS friends
OPTIONAL MATCH (friend)<-[:hasCreator]-(post)<-[:containerOf]-(forum)
WHERE
    friend IN friends
WITH
    forum,
    count(post) AS postCount
RETURN
    forum.title AS forumName,
    postCount
ORDER BY
    postCount DESC,
    forum.id ASC
LIMIT 20''',
    "Interactive Complex 6" : '''MATCH (knownTag:Tag { name: $tagName})
WITH knownTag.id as knownTagId

MATCH (person:Person {id: $personId1})-[:knows*1..2]-(friend)
WHERE NOT person=friend
WITH
    knownTagId,
    collect(distinct friend) as friends
UNWIND friends as f
    MATCH (f)<-[:hasCreator]-(post:Post),
          (post)-[:hasTag]->(t:Tag{id: knownTagId}),
          (post)-[:hasTag]->(tag:Tag)
    WHERE NOT t = tag
    WITH
        tag.name as tagName,
        count(post) as postCount
RETURN
    tagName,
    postCount
ORDER BY
    postCount DESC,
    tagName ASC
LIMIT 10''',
    "Interactive Complex 7" : '''MATCH (person:Person {id: $personId1})<-[:hasCreator]-(message:Message)<-[like:likes]-(liker:Person)
    WITH liker, message, like.creationDate AS likeTime, person
    ORDER BY likeTime DESC, toInteger(message.id) ASC
    WITH liker, head(collect({msg: message, likeTime: likeTime})) AS latestLike, person
RETURN
    liker.id AS personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    latestLike.likeTime AS likeCreationDate,
    latestLike.msg.id AS commentOrPostId,
    coalesce(latestLike.msg.content, latestLike.msg.imageFile) AS commentOrPostContent,
    toInteger(floor(toFloat(toInteger(latestLike.likeTime) - toInteger(latestLike.msg.creationDate))/1000.0)/60.0) AS minutesLatency,
    not((liker)-[:knows]-(person)) AS isNew
ORDER BY
    likeCreationDate DESC,
    toInteger(personId) ASC
LIMIT 20''',
    "Interactive Complex 8" : '''MATCH (start:Person {id: $personId1})<-[:hasCreator]-(:Message)<-[:replyOf]-(comment:Comment)-[:hasCreator]->(person:Person)
RETURN
    person.id AS personId,
    person.firstName AS personFirstName,
    person.lastName AS personLastName,
    comment.creationDate AS commentCreationDate,
    comment.id AS commentId,
    comment.content AS commentContent
ORDER BY
    commentCreationDate DESC,
    commentId ASC
LIMIT 20''',
    "Interactive Complex 9" : '''MATCH (root:Person {id: $personId1 })-[:knows*1..2]-(friend:Person)
WHERE NOT friend = root
WITH collect(distinct friend) as friends
UNWIND friends as friend
    MATCH (friend)<-[:hasCreator]-(message:Message)
    WHERE message.creationDate < $maxDate
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    message.id AS commentOrPostId,
    coalesce(message.content,message.imageFile) AS commentOrPostContent,
    message.creationDate AS commentOrPostCreationDate
ORDER BY
    commentOrPostCreationDate DESC,
    message.id ASC
LIMIT 20''',
    "Interactive Complex 10" : '''MATCH (person:Person {id: $personId1})-[:knows*2..2]-(friend),
       (friend)-[:isLocatedIn]->(city:Place)
WHERE NOT friend=person AND
      NOT (friend)-[:knows]-(person)
WITH person, city, friend, datetime({epochMillis: toInteger(friend.birthday)}) as birthday
WHERE  (birthday.month=$month AND birthday.day>=21) OR
        (birthday.month=($month%12)+1 AND birthday.day<22)
WITH DISTINCT friend, city, person
OPTIONAL MATCH (friend)<-[:hasCreator]-(post:Post)
WITH friend, city, collect(post) AS posts, person
WITH friend,
     city,
     size(posts) AS postCount,
     size([p IN posts WHERE (p)-[:hasTag]->()<-[:hasInterest]-(person)]) AS commonPostCount
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
       friend.gender AS personGender,
       city.name AS personCityName
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 10''',
    "Interactive Complex 11" : '''MATCH (person:Person {id: $personId1})-[:knows*1..2]-(friend:Person)
WHERE not(person=friend)
WITH DISTINCT friend
MATCH (friend)-[workAt:workAt]->(company:Organisation)-[:isLocatedIn]->(:Place {name: $countryName11})
WHERE workAt.workFrom < $workFromYear11
RETURN
        friend.id AS personId,
        friend.firstName AS personFirstName,
        friend.lastName AS personLastName,
        company.name AS organizationName,
        workAt.workFrom AS organizationWorkFromYear
ORDER BY
        organizationWorkFromYear ASC,
        toInteger(personId) ASC,
        organizationName DESC
LIMIT 10''',
    "Interactive Complex 12" : '''MATCH (tag:Tag)-[:hasType|isSubclassOf*0..]->(baseTagClass:TagClass)
WHERE tag.name = $tagClassName12 OR baseTagClass.name = $tagClassName12
WITH collect(tag.id) as tags
MATCH (:Person {id: $personId1})-[:knows]-(friend:Person)<-[:hasCreator]-(comment:Comment)-[:replyOf]->(:Post)-[:hasTag]->(tag:Tag)
WHERE tag.id in tags
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    collect(DISTINCT tag.name) AS tagNames,
    count(DISTINCT comment) AS replyCount
ORDER BY
    replyCount DESC,
    toInteger(personId) ASC
LIMIT 20''',
    "Interactive Complex 13" : '''MATCH
    (person1:Person {id: $person1Id13}),
    (person2:Person {id: $person2Id13}),
    path = shortestPath((person1)-[:knows*]-(person2))
RETURN
    CASE path IS NULL
        WHEN true THEN -1
        ELSE length(path)
    END AS shortestPathLength''',
    "Interactive Complex 14" : '''MATCH path = allShortestPaths((person1:Person { id: $person1Id13 })-[:knows*0..]-(person2:Person { id: $person2Id13 }))
WITH collect(path) as paths
UNWIND paths as path
WITH path, relationships(path) as rels_in_path
WITH
    [n in nodes(path) | n.id ] as personIdsInPath,
    [r in rels_in_path |
        reduce(w=0.0, v in [
            (a:Person)<-[:hasCreator]-(:Comment)-[:replyOf]->(:Post)-[:hasCreator]->(b:Person)
            WHERE
                (a.id = startNode(r).id and b.id=endNode(r).id) OR (a.id=endNode(r).id and b.id=startNode(r).id)
            | 1.0] | w+v)
    ] as weight1,
    [r in rels_in_path |
        reduce(w=0.0,v in [
        (a:Person)<-[:hasCreator]-(:Comment)-[:replyOf]->(:Comment)-[:hasCreator]->(b:Person)
        WHERE
                (a.id = startNode(r).id and b.id=endNode(r).id) OR (a.id=endNode(r).id and b.id=startNode(r).id)
        | 0.5] | w+v)
    ] as weight2
WITH
    personIdsInPath,
    reduce(w=0.0,v in weight1| w+v) as w1,
    reduce(w=0.0,v in weight2| w+v) as w2
RETURN
    personIdsInPath,
    (w1+w2) as pathWeight
ORDER BY pathWeight desc''',
}

total_time = 0
lowest = 10000000000000000000000; lowest_name = ""
highest = 0; highest_name = ""
query_count = 0 

print("#---------------------------------------------------------------------------#")
# print("#---------------- Benchmarking Queries: Interactive Update -----------------#")
print("#---------------Benchmarking Queries: Interactive Complex-----------------#")
# print("#---------------- Benchmarking Queries: Interactive Shorts -----------------#")
print("#---------------------------------------------------------------------------#")

with driver.session() as session:
    # for quer, q in i_shorts.items():
    # for quer, q in i_complex.items():
    for quer, q in i_updates.items():
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

