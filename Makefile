include .env

init_kg:
	docker exec -it kg-db cypher-shell -u ${NEO4J_USER} -p ${NEO4J_PASSWORD} -f /var/lib/neo4j/import/init_kg/init_kg.cypher

test:
	coverage run --source=./src -m unittest discover -s tests/ && coverage report -m