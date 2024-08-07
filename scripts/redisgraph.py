import redis
from redis.commands.graph.edge import Edge
from redis.commands.graph.node import Node
import csv
import time

# Connect to a database
r = redis.Redis(host="localhost", port=12000)


# Define a graph called SocialMedia
social_graph = r.graph("Mygraph")


#function to process a csv
def load_csv_to_redisgraph(file_path, delimiter, process_row):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file, delimiter=delimiter)
        for row in reader:
            process_row(row)

def create_node(label, properties):
    node = Node(label=label, properties=properties)
    social_graph.add_node(node)
    return node

# Function to create edges
def create_edge(src_node, relation, dst_node, properties=None):
    edge = Edge(src_node, relation, dst_node, properties=properties)
    social_graph.add_edge(edge)

nodes={}


def process_organisation_row(row):
    properties = {
        'id': int(row['id']),
        'type': row['type'],
        'name': row['name'],
        'url': row['url']
    }
    node = create_node('Organisation', properties)
    nodes[(row['id'], 'Organisation')] = node

start = time.time()

load_csv_to_redisgraph('../static/organisation_0_0.csv', '|', process_organisation_row)

def process_place_row(row):
    properties = {
        'id': int(row['id']),
        'name': row['name'],
        'url': row['url'],
        'type': row['type']
    }
    node = create_node('Place', properties)
    nodes[(row['id'], 'Place')] = node

load_csv_to_redisgraph('../static/place_0_0.csv', '|', process_place_row)

def process_tagclass_row(row):
    properties = {
        'id': int(row['id']),
        'name': row['name'],
        'url': row['url']
    }
    node = create_node('TagClass', properties)
    nodes[(row['id'], 'TagClass')] = node

load_csv_to_redisgraph('../static/tagclass_0_0.csv', '|', process_tagclass_row)


def process_tag_row(row):
    properties = {
        'id': int(row['id']),
        'name': row['name'],
        'url': row['url']
    }
    node = create_node('Tag', properties)
    nodes[(row['id'], 'Tag')] = node

load_csv_to_redisgraph('../static/tag_0_0.csv', '|', process_tag_row)

#Load Organisation isLocatedIn Place relationships
def process_org_islocatedin_place_row(row):
    org = nodes[(row['Organisation.id'], 'Organisation')]
    place = nodes[(row['Place.id'], 'Place')]
    create_edge(org, 'isLocatedIn', place)

load_csv_to_redisgraph('../static/organisation_isLocatedIn_place_0_0.csv', '|', process_org_islocatedin_place_row)

# Load Place isPartOf Place relationships
def process_place_ispartof_place_row(row):
    place1 = nodes[(row['Place1.id'], 'Place')]
    place2 = nodes[(row['Place2.id'], 'Place')]
    create_edge(place1, 'isPartOf', place2)

load_csv_to_redisgraph('../static/place_isPartOf_place_0_0.csv', '|', process_place_ispartof_place_row)

# Load Tag hasType TagClass relationships
def process_tag_hastype_tagclass_row(row):
    tag = nodes[(row['Tag.id'], 'Tag')]
    tagclass = nodes[(row['TagClass.id'], 'TagClass')]
    create_edge(tag, 'hasType', tagclass)

load_csv_to_redisgraph('../static/tag_hasType_tagclass_0_0.csv', '|', process_tag_hastype_tagclass_row)

# Load TagClass isSubclassOf TagClass relationships
def process_tagclass_issubclassof_tagclass_row(row):
    tagclass1 = nodes[(row['TagClass1.id'], 'TagClass')]
    tagclass2 = nodes[(row['TagClass2.id'], 'TagClass')]
    create_edge(tagclass1, 'isSubclassOf', tagclass2)

load_csv_to_redisgraph('../static/tagclass_isSubclassOf_tagclass_0_0.csv', '|', process_tagclass_issubclassof_tagclass_row)

# # Load Person nodes
# def process_person_row(row):
#     properties = {
#         'id': int(row['id']),
#         'firstName': row['firstName'],
#         'lastName': row['lastName'],
#         'gender': row['gender'],
#         'birthday': row['birthday'],
#         'creationDate': row['creationDate'],
#         'locationIP': row['locationIP'],
#         'browserUsed': row['browserUsed'],
#         'language': row['language'],
#         'email': row['email']
#     }
#     node = create_node('Person', properties)
#     nodes[(row['id'], 'Person')] = node

# load_csv_to_redisgraph('../dynamic/person_0_0.csv', '|', process_person_row)

# # Load Forum nodes
# def process_forum_row(row):
#     properties = {
#         'id': int(row['id']),
#         'title': row['title'],
#         'creationDate': row['creationDate']
#     }
#     node = create_node('Forum', properties)
#     nodes[(row['id'], 'Forum')] = node

# load_csv_to_redisgraph('../dynamic/forum_0_0.csv', '|', process_forum_row)

# # Load Post nodes
# def process_post_row(row):
#     properties = {
#         'id': int(row['id']),
#         'imageFile': row['imageFile'] if row['imageFile'] else None,
#         'creationDate': row['creationDate'],
#         'locationIP': row['locationIP'],
#         'browserUsed': row['browserUsed'],
#         'language': row['language'],
#         'content': row['content'],
#         'length': int(row['length'])
#     }
#     node = create_node('Post', properties)
#     nodes[(row['id'], 'Post')] = node

# load_csv_to_redisgraph('../dynamic/post_0_0.csv', '|', process_post_row)

# # Load Comment nodes
# def process_comment_row(row):
#     properties = {
#         'id': int(row['id']),
#         'creationDate': row['creationDate'],
#         'locationIP': row['locationIP'],
#         'browserUsed': row['browserUsed'],
#         'content': row['content'],
#         'length': int(row['length'])
#     }
#     node = create_node('Comment', properties)
#     nodes[(row['id'], 'Comment')] = node

# load_csv_to_redisgraph('../dynamic/comment_0_0.csv', '|', process_comment_row)

# # Load Forum hasMember Person relationships
# def process_forum_has_member_row(row):
#     forum = nodes[(row['Forum.id'], 'Forum')]
#     person = nodes[(row['Person.id'], 'Person')]
#     create_edge(forum, 'hasMember', person, {'creationDate': row['joinDate']})

# load_csv_to_redisgraph('../dynamic/forum_hasMember_person_0_0.csv', '|', process_forum_has_member_row)

# # Load Forum hasModerator Person relationships
# def process_forum_has_moderator_row(row):
#     forum = nodes[(row['Forum.id'], 'Forum')]
#     person = nodes[(row['Person.id'], 'Person')]
#     create_edge(forum, 'hasModerator', person)

# load_csv_to_redisgraph('../dynamic/forum_hasModerator_person_0_0.csv', '|', process_forum_has_moderator_row)

# # Load Forum hasTag relationships
# def process_forum_has_tag_row(row):
#     forum = nodes[(row['Forum.id'], 'Forum')]
#     tag = nodes[(row['Tag.id'], 'Tag')]
#     create_edge(forum, 'hasTag', tag)

# load_csv_to_redisgraph('../dynamic/forum_hasTag_tag_0_0.csv', '|', process_forum_has_tag_row)

# # Load Forum containerOf Post relationships
# def process_forum_containerof_post_row(row):
#     forum = nodes[(row['Forum.id'], 'Forum')]
#     post = nodes[(row['Post.id'], 'Post')]
#     create_edge(forum, 'containerOf', post)

# load_csv_to_redisgraph('../dynamic/forum_containerOf_post_0_0.csv', '|', process_forum_containerof_post_row)

# # Load Post hasTag relationships
# def process_post_has_tag_row(row):
#     post = nodes[(row['Post.id'], 'Post')]
#     tag = nodes[(row['Tag.id'], 'Tag')]
#     create_edge(post, 'hasTag', tag)

# load_csv_to_redisgraph('../dynamic/post_hasTag_tag_0_0.csv', '|', process_post_has_tag_row)

# # Load Post isLocatedIn Place relationships
# def process_post_islocatedin_place_row(row):
#     post = nodes[(row['Post.id'], 'Post')]
#     place = nodes[(row['Place.id'], 'Place')]
#     create_edge(post, 'isLocatedIn', place)

# load_csv_to_redisgraph('../dynamic/post_isLocatedIn_place_0_0.csv', '|', process_post_islocatedin_place_row)

# # Load Post hasCreator Person relationships
# def process_post_hascreator_person_row(row):
#     post = nodes[(row['Post.id'], 'Post')]
#     person = nodes[(row['Person.id'], 'Person')]
#     create_edge(post, 'hasCreator', person)

# load_csv_to_redisgraph('../dynamic/post_hasCreator_person_0_0.csv', '|', process_post_hascreator_person_row)

# # Load Comment hasCreator Person relationships
# def process_comment_hascreator_person_row(row):
#     comment = nodes[(row['Comment.id'], 'Comment')]
#     person = nodes[(row['Person.id'], 'Person')]
#     create_edge(comment, 'hasCreator', person)

# load_csv_to_redisgraph('../dynamic/comment_hasCreator_person_0_0.csv', '|', process_comment_hascreator_person_row)

# # Load Comment isLocatedIn Place relationships
# def process_comment_islocatedin_place_row(row):
#     comment = nodes[(row['Comment.id'], 'Comment')]
#     place = nodes[(row['Place.id'], 'Place')]
#     create_edge(comment, 'isLocatedIn', place)

# load_csv_to_redisgraph('../dynamic/comment_isLocatedIn_place_0_0.csv', '|', process_comment_islocatedin_place_row)

# # Load Person studyAt University relationships
# def process_person_studyat_university_row(row):
#     person = nodes[(row['Person.id'], 'Person')]
#     university = nodes[(row['Organisation.id'], 'Organisation')]
#     create_edge(person, 'studyAt', university, {'classYear': int(row['classYear'])})

# load_csv_to_redisgraph('../dynamic/person_studyAt_organisation_0_0.csv', '|', process_person_studyat_university_row)

# # Load Person workAt Company relationships
# def process_person_workat_company_row(row):
#     person = nodes[(row['Person.id'], 'Person')]
#     company = nodes[(row['Organisation.id'], 'Organisation')]
#     create_edge(person, 'workAt', company, {'workFrom': int(row['workFrom'])})

# load_csv_to_redisgraph('../dynamic/person_workAt_organisation_0_0.csv', '|', process_person_workat_company_row)



end_time = time.time()

print("The total time taken is",end_time-start)

social_graph.commit()