#!/bin/bash

# turn on bash's job control
set -m

# Start the primary process and put it in the background
/startup/docker-entrypoint.sh neo4j &

# Init index and node
source /scripts/neo4j_init.sh
chmod +x /scripts/neo4j_init.sh

execute_cypher "/var/lib/neo4j/import/constraints.cypher" "Create Index" 3
execute_cypher "/var/lib/neo4j/import/nodes.cypher" "Create Nodes" 3

fg %1