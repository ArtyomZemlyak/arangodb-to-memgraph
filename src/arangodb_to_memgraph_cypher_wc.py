import os
import json
from time import time

from arango import ArangoClient
from gqlalchemy import Memgraph

from ai_common_utils.files import load_env_file
from tqdm import tqdm


def open_json(path: str):
    return json.loads(open(path, "r", encoding="utf8").read())


load_env_file(".env")
ARANGODB_URL = os.environ.get("ARANGODB_URL")
ARANGODB_USER = os.environ.get("ARANGODB_USER")
ARANGODB_PASS = os.environ.get("ARANGODB_PASS")

MEMGRAPH_HOST = os.environ.get("MEMGRAPH_HOST")
MEMGRAPH_PORT = os.environ.get("MEMGRAPH_PORT")

# Initialize the client for ArangoDB.
client = ArangoClient(hosts=ARANGODB_URL)

# Connect to "irkutsk" database as root user.
db = client.db("irkutsk", username=ARANGODB_USER, password=ARANGODB_PASS)
aql = db.aql


def get_collection(name: str):
    cursor = aql.execute(
        f"""
    FOR doc IN {name}
        RETURN doc
    """
    )
    return [doc for doc in cursor]


# tags = get_collection("tags")
services = get_collection("services")
# depends_and = get_collection("depends_and")
depends_or = get_collection("depends_or")


# Make a connection to the database:
memgraph = Memgraph(host=MEMGRAPH_HOST, port=int(MEMGRAPH_PORT))


BAD_SET = set(["_key", "_id", "_rev", "id"])


# Create cypher query for adding graph to memgraph:

newline = "\n"

# TODO: different types of v
def nodes_to_cypher(nodes_collection: list, collection_name: str):
    return "\n".join(
        [
            f"""CREATE ({collection_name[0]}:{collection_name} {{id: "{node['_key']}", {', '.join([f'''{k}: "{str(v).replace('"', "'").replace(newline, "")}"''' for k, v in node.items() if k not in BAD_SET])}}});"""
            for node in nodes_collection
        ]
    )


set_from_to = set([])

for edge in depends_or:
    set_from_to.add(edge["_from"])
    set_from_to.add(edge["_to"])

print(len(services))
services = [service for service in services if service["_id"] not in set_from_to]
print(len(services))


services = nodes_to_cypher(services, "services")

for query in tqdm(services.split("\n")):
    # print(query)
    memgraph.execute(query)


CYPHER_QUERY_EXAMPLE = """
CREATE (p:Person {id: "100", name: "Daniel", age: 30, city: "London"});
CREATE (p:Person {id: "101", name: "Alex", age: 15, city: "Paris"});
CREATE (p:Person {id: "102", name: "Sarah", age: 17, city: "London"});
CREATE (p:Person {id: "103", name: "Mia", age: 25, city: "Zagreb"});
CREATE (p:Person {id: "104", name: "Lucy", age: 21, city: "Paris"});
CREATE (r:Restaurant {id: "200", name: "Mc Donalds", menu: "Fries BigMac McChicken Apple Pie"});
CREATE (r:Restaurant {id: "201", name: "KFC", menu: "Fried Chicken Fries Chicken Bucket"});
CREATE (r:Restaurant {id: "202", name: "Subway", menu: "Ham Sandwich Turkey Sandwich Foot-long"});
CREATE (r:Restaurant {id: "203", name: "Dominos", menu: "Pepperoni Pizza Double Dish Pizza Cheese filled Crust"});
MATCH (u:Person), (v:Person) WHERE u.id = "100" AND v.id = "103" CREATE (u)-[:IS_FRIENDS_WITH {met_in: "2014"}]->(v);
MATCH (u:Person), (v:Person) WHERE u.id = "101" AND v.id = "104" CREATE (u)-[:IS_FRIENDS_WITH {met_in: "2001"}]->(v);
MATCH (u:Person), (v:Person) WHERE u.id = "102" AND v.id = "100" CREATE (u)-[:IS_FRIENDS_WITH {met_in: "2005"}]->(v);
MATCH (u:Person), (v:Person) WHERE u.id = "102" AND v.id = "103" CREATE (u)-[:IS_FRIENDS_WITH {met_in: "2017"}]->(v);
MATCH (u:Person), (v:Person) WHERE u.id = "103" AND v.id = "104" CREATE (u)-[:IS_FRIENDS_WITH {met_in: "2005"}]->(v);
MATCH (u:Person), (v:Person) WHERE u.id = "104" AND v.id = "102" CREATE (u)-[:IS_FRIENDS_WITH {met_in: "2021"}]->(v);
MATCH (u:Person), (v:Restaurant) WHERE u.id = "100" AND v.id = "200" CREATE (u)-[:ATE_AT {liked: true}]->(v);
MATCH (u:Person), (v:Restaurant) WHERE u.id = "102" AND v.id = "202" CREATE (u)-[:ATE_AT {liked: false}]->(v);
MATCH (u:Person), (v:Restaurant) WHERE u.id = "102" AND v.id = "203" CREATE (u)-[:ATE_AT {liked: false}]->(v);
MATCH (u:Person), (v:Restaurant) WHERE u.id = "102" AND v.id = "200" CREATE (u)-[:ATE_AT {liked: true}]->(v);
MATCH (u:Person), (v:Restraunt) WHERE u.id = "103" AND v.id = "201" CREATE (u)-[:ATE_AT {liked: true}]->(v);
MATCH (u:Person), (v:Restaurant) WHERE u.id = "104" AND v.id = "201" CREATE (u)-[:ATE_AT {liked: false}]->(v);
MATCH (u:Person), (v:Restaurant) WHERE u.id = "101" AND v.id = "200" CREATE (u)-[:ATE_AT {liked: true}]->(v);
"""
