#!/bin/bash

# Function to execute a cypher file with custom messages
execute_cypher() {
  local file_path=$1          # File path passed as a parameter
  local operation_message=$2  # Custom message for the operation
  local max_retries=${3:-5}   # Default max retries to 5 if not provided
  local retry_count=0         # Initialize retry counter

  echo "Starting operation: $operation_message using file: $file_path"

  # Retry loop
  until cypher-shell -u "$NEO4J_USER" -p "$NEO4J_PASSWORD" --file "$file_path"
  do
    echo "$operation_message failed, retrying in 10 seconds..."
    sleep 10

    # Increment retry counter
    ((retry_count++))

    # Check if retry count reached the limit
    if [ "$retry_count" -ge "$max_retries" ]; then
      echo "Maximum retry limit ($max_retries) reached. $operation_message failed."
      return 1  # Exit the function with failure
    fi
  done

  echo "$operation_message succeeded using file: $file_path"
  return 0  # Exit the function with success
}

# Check function return status for each operation
if [ $? -ne 0 ]; then
  echo "Error: Operation failed."
else
  echo "Success: Operation completed successfully."
fi


# until
# cypher-shell -u $NEO4J_USER -p $NEO4J_PASSWORD --file /var/lib/neo4j/import/constraints.cypher
# do
#   echo "Create index failed, sleeping"
#   sleep 10
# done

# until
# cypher-shell -u $NEO4J_USER -p $NEO4J_PASSWORD --file /var/lib/neo4j/import/nodes.cypher
# do
#   echo "Create node failed, sleeping"
#   sleep 10
# done