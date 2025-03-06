# frozen_string_literal: true

require 'aws-sdk-kinesis'
require 'aws-sdk-dynamodb'

# rubocop:disable Metrics/ModuleLength
module LocalstackHelper
  LOCALSTACK_ENDPOINT = 'http://localhost:4566'
  LOCALSTACK_REGION = 'us-east-1' # Explicitly define the region
  LOCALSTACK_CREDENTIALS = Aws::Credentials.new('test', 'test')

  def self.localstack_options(service_type = nil)
    options = {
      endpoint: LOCALSTACK_ENDPOINT,
      region: LOCALSTACK_REGION,
      credentials: LOCALSTACK_CREDENTIALS,
      http_wire_trace: true # Add debug tracing
    }

    # We'll keep this logic but it won't be used since we don't need S3
    options[:force_path_style] = true if service_type == :s3

    options
  end

  def self.kinesis_client
    # More direct configuration for Localstack compatibility
    Aws::Kinesis::Client.new(
      endpoint: 'http://localhost:4566',
      region: 'us-east-1',
      credentials: Aws::Credentials.new('test', 'test'),
      ssl_verify_peer: false
    )
  end

  def self.dynamodb_client
    # Match the same pattern for consistency
    Aws::DynamoDB::Client.new(
      endpoint: LOCALSTACK_ENDPOINT,
      region: LOCALSTACK_REGION,
      credentials: LOCALSTACK_CREDENTIALS,
      ssl_verify_peer: false, # Disable SSL verification for Localstack
      http_wire_trace: true
    )
  end

  def self.create_test_stream(stream_name, shard_count)
    client = kinesis_client
    begin
      client.create_stream(
        stream_name: stream_name,
        shard_count: shard_count
      )
    rescue Aws::Kinesis::Errors::ResourceInUseException
      # Stream already exists, which is fine
    end
    client
  end

  # rubocop:disable Metrics/MethodLength
  def self.create_dynamodb_table(table_name)
    client = dynamodb_client

    # First check if the table already exists
    begin
      client.describe_table(table_name: table_name)
      puts "DynamoDB table #{table_name} already exists"
      return
    rescue Aws::DynamoDB::Errors::ResourceNotFoundException
      # Table doesn't exist, continue to creation
    end

    puts "Creating DynamoDB table #{table_name}..."

    # Create table with schema that matches the State class's expectations
    create_params = {
      table_name: table_name,
      attribute_definitions: [
        {
          attribute_name: 'consumerGroup',
          attribute_type: 'S'
        },
        {
          attribute_name: 'streamName',
          attribute_type: 'S'
        }
      ],
      key_schema: [
        {
          attribute_name: 'consumerGroup',
          key_type: 'HASH'
        },
        {
          attribute_name: 'streamName',
          key_type: 'RANGE'
        }
      ],
      provisioned_throughput: {
        read_capacity_units: 5,
        write_capacity_units: 5
      }
    }

    client.create_table(create_params)

    puts 'Waiting for table to be active...'
    # Wait for the table to be active
    begin
      client.wait_until(:table_exists, table_name: table_name)
      puts 'Table is now active'
    rescue StandardError => e
      puts "Error waiting for table to be active: #{e.message}"
    end

    # Pre-create an initial item with shards map to avoid update failures
    puts 'Pre-creating initial item with empty shards map...'
    begin
      client.put_item(
        table_name: table_name,
        item: {
          'consumerGroup' => 'test-group',
          'streamName' => 'test-stream',
          'shards' => {} # Empty map for shards
        }
      )
      puts 'Initial item created successfully'
    rescue StandardError => e
      puts "Error creating initial item: #{e.message}"
    end
  end
  # rubocop:enable Metrics/MethodLength

  def self.put_test_records(stream_name, records)
    client = kinesis_client
    created_records = []
    records.each do |record_data|
      record = client.put_record(
        stream_name: stream_name,
        data: record_data,
        partition_key: 'test-key'
      )
      created_records << record
    end
    puts "Created records: #{created_records.inspect}"
    created_records
  end
end
# rubocop:enable Metrics/ModuleLength
