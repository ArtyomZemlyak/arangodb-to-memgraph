import os
import json
from time import time

from gqlalchemy import Memgraph

from ai_common_utils.files import load_env_file


load_env_file(".env")


MEMGRAPH_HOST = os.environ.get("MEMGRAPH_HOST")
MEMGRAPH_PORT = os.environ.get("MEMGRAPH_PORT")


# Make a connection to the database:
memgraph = Memgraph(host=MEMGRAPH_HOST, port=int(MEMGRAPH_PORT))


memgraph.drop_database()
