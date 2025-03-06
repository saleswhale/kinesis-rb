#!/bin/sh
set -e

# Wait for Localstack to be ready
echo "Waiting for Localstack to become available..."
ATTEMPTS=0
MAX_ATTEMPTS=30
CONTAINER_NAME="kinesis-localstack"

until $(curl --silent --fail http://$CONTAINER_NAME:4566/health > /dev/null 2>&1); do
  ATTEMPTS=$((ATTEMPTS + 1))
  if [ $ATTEMPTS -ge $MAX_ATTEMPTS ]; then
    echo "Localstack didn't become available in time"
    exit 1
  fi
  echo "Waiting for Localstack... (attempt $ATTEMPTS/$MAX_ATTEMPTS)"
  sleep 2
done

echo "Localstack is available! Setting up resources..."

# Create Kinesis stream
echo 'Creating Kinesis stream...'
aws --endpoint-url=http://$CONTAINER_NAME:4566 kinesis create-stream --stream-name test-stream --shard-count 2 || true

# Wait for stream to become active
echo 'Waiting for stream to become active...'
sleep 5

# Create DynamoDB table
echo 'Creating DynamoDB table...'
aws --endpoint-url=http://$CONTAINER_NAME:4566 dynamodb create-table --table-name test-table --attribute-definitions AttributeName=stream_name,AttributeType=S AttributeName=shard_id,AttributeType=S --key-schema AttributeName=stream_name,KeyType=HASH AttributeName=shard_id,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 || true

# Put test records
echo 'Putting test records...'
aws --endpoint-url=http://$CONTAINER_NAME:4566 kinesis put-record --stream-name test-stream --partition-key test-key --data $(echo -n 'test record 1' | base64)
aws --endpoint-url=http://$CONTAINER_NAME:4566 kinesis put-record --stream-name test-stream --partition-key test-key --data $(echo -n 'test record 2' | base64)
aws --endpoint-url=http://$CONTAINER_NAME:4566 kinesis put-record --stream-name test-stream --partition-key test-key --data $(echo -n 'test record 3' | base64)

echo 'Setup complete!' 