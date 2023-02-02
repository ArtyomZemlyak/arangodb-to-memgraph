import os
import json
from time import time

from arangodb_client.client import ArangoDBClient
from gqlalchemy import Memgraph

from ai_common_utils.files import load_env_file
from tqdm import tqdm


def open_json(path: str):
    return json.loads(open(path, "r", encoding="utf8").read())


load_env_file(".env")
ARANGODB_URL = os.environ.get("ARANGODB_URL")
ARANGODB_USER = os.environ.get("ARANGODB_USER")
ARANGODB_PASS = os.environ.get("ARANGODB_PASS")
# DBNAME = "gosuslugi_passport_v1"
DBNAME = "mfc_v1"

MEMGRAPH_HOST = os.environ.get("MEMGRAPH_HOST")
MEMGRAPH_PORT = os.environ.get("MEMGRAPH_PORT")


# Initialize the client for ArangoDB.
client = ArangoDBClient(
    hosts=ARANGODB_URL,
    username=ARANGODB_USER,
    password=ARANGODB_PASS,
)

client.connect_to_db(db_name=DBNAME)


def get_collections(type_collection: str = "document"):
    collections = client.data.get_all_collections_info()
    return {
        collection["name"]: [
            doc for doc in client.data.get_collection(collection["name"])
        ]
        for collection in collections
        if collection["type"] == type_collection
    }


# Get all collections data:
vertexies = get_collections("document")
edges = get_collections("edge")


# Make a connection to the memgraph database:
memgraph = Memgraph(host=MEMGRAPH_HOST, port=int(MEMGRAPH_PORT))


BAD_SET = set(["_key", "_id", "_rev", "id", "embedding", "embedding_nvc"])
newline = "\n"


# Create cypher query for adding graph to memgraph:


def format_value(v):
    return str(v).replace('"', "'").replace(newline, "").replace("\\", "")


# TODO: different types of v
def nodes_to_cypher(nodes_collection: list, collection_name: str):
    return [
        f"""CREATE ({collection_name[0]}:{collection_name} {{id: "{node['_key']}", {', '.join([f'''{k}: "{format_value(v)}"''' for k, v in node.items() if k not in BAD_SET])}}});"""
        for node in nodes_collection
    ]


# TODO: parameters for edges
def edges_to_cypher(edges_collection: list, collection_name: str):
    return [
        f"""MATCH (u:{edge['_from'].split('/')[0]}), (v:{edge['_to'].split('/')[0]}) WHERE u.id = "{edge['_from'].split('/')[1]}" AND v.id = "{edge['_to'].split('/')[1]}" CREATE (u)-[:{collection_name.upper()}]->(v);"""
        for edge in edges_collection
    ]


print("Load vertexies:")
for i, collection in enumerate(vertexies.items()):
    collection_name, collection_docs = collection
    print(f"{i+1}/{len(vertexies)}: {collection_name}")

    for query in tqdm(nodes_to_cypher(collection_docs, collection_name)):
        # print(query)
        try:
            memgraph.execute(query)
        except Exception:
            print(query)
            raise Exception


print("Load edges:")
for i, collection in enumerate(edges.items()):
    collection_name, collection_docs = collection
    print(f"{i+1}/{len(edges)}: {collection_name}")

    for query in tqdm(edges_to_cypher(collection_docs, collection_name)):
        # print(query)
        memgraph.execute(query)


# CYPHER_QUERY_EXAMPLE = """
# CREATE (p:Person {id: "100", name: "Daniel", age: 30, city: "London"});
# CREATE (p:Person {id: "101", name: "Alex", age: 15, city: "Paris"});
# CREATE (p:Person {id: "102", name: "Sarah", age: 17, city: "London"});
# CREATE (p:Person {id: "103", name: "Mia", age: 25, city: "Zagreb"});
# CREATE (p:Person {id: "104", name: "Lucy", age: 21, city: "Paris"});
# CREATE (r:Restaurant {id: "200", name: "Mc Donalds", menu: "Fries BigMac McChicken Apple Pie"});
# CREATE (r:Restaurant {id: "201", name: "KFC", menu: "Fried Chicken Fries Chicken Bucket"});
# CREATE (r:Restaurant {id: "202", name: "Subway", menu: "Ham Sandwich Turkey Sandwich Foot-long"});
# CREATE (r:Restaurant {id: "203", name: "Dominos", menu: "Pepperoni Pizza Double Dish Pizza Cheese filled Crust"});
# MATCH (u:Person), (v:Person) WHERE u.id = "100" AND v.id = "103" CREATE (u)-[:IS_FRIENDS_WITH {met_in: "2014"}]->(v);
# MATCH (u:Person), (v:Person) WHERE u.id = "101" AND v.id = "104" CREATE (u)-[:IS_FRIENDS_WITH {met_in: "2001"}]->(v);
# MATCH (u:Person), (v:Person) WHERE u.id = "102" AND v.id = "100" CREATE (u)-[:IS_FRIENDS_WITH {met_in: "2005"}]->(v);
# MATCH (u:Person), (v:Person) WHERE u.id = "102" AND v.id = "103" CREATE (u)-[:IS_FRIENDS_WITH {met_in: "2017"}]->(v);
# MATCH (u:Person), (v:Person) WHERE u.id = "103" AND v.id = "104" CREATE (u)-[:IS_FRIENDS_WITH {met_in: "2005"}]->(v);
# MATCH (u:Person), (v:Person) WHERE u.id = "104" AND v.id = "102" CREATE (u)-[:IS_FRIENDS_WITH {met_in: "2021"}]->(v);
# MATCH (u:Person), (v:Restaurant) WHERE u.id = "100" AND v.id = "200" CREATE (u)-[:ATE_AT {liked: true}]->(v);
# MATCH (u:Person), (v:Restaurant) WHERE u.id = "102" AND v.id = "202" CREATE (u)-[:ATE_AT {liked: false}]->(v);
# MATCH (u:Person), (v:Restaurant) WHERE u.id = "102" AND v.id = "203" CREATE (u)-[:ATE_AT {liked: false}]->(v);
# MATCH (u:Person), (v:Restaurant) WHERE u.id = "102" AND v.id = "200" CREATE (u)-[:ATE_AT {liked: true}]->(v);
# MATCH (u:Person), (v:Restraunt) WHERE u.id = "103" AND v.id = "201" CREATE (u)-[:ATE_AT {liked: true}]->(v);
# MATCH (u:Person), (v:Restaurant) WHERE u.id = "104" AND v.id = "201" CREATE (u)-[:ATE_AT {liked: false}]->(v);
# MATCH (u:Person), (v:Restaurant) WHERE u.id = "101" AND v.id = "200" CREATE (u)-[:ATE_AT {liked: true}]->(v);
# """
