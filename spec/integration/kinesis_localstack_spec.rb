# frozen_string_literal: true

require 'aws-sdk-core'
require 'spec_helper'
require 'support/localstack_helper'

# Set environment variable for testing
ENV['KINESIS_CONSUMER_ID'] = 'test-consumer-id'
ENV['AWS_ACCESS_KEY_ID'] = 'test'
ENV['AWS_SECRET_ACCESS_KEY'] = 'test'
ENV['AWS_REGION'] = 'us-east-1'

# Special Localstack environment settings
ENV['LOCALSTACK_HOSTNAME'] = 'localhost'
ENV['AWS_CBOR_DISABLE'] = 'true' # Disable CBOR protocol which can cause issues
ENV['AWS_EC2_METADATA_DISABLED'] = 'true' # Disable EC2 metadata service lookup
ENV['AWS_ENDPOINT_URL'] = 'http://localhost:4566'

# Configure the AWS SDK globally
Aws.config.update(
  endpoint: 'http://localhost:4566',
  region: 'us-east-1',
  credentials: Aws::Credentials.new('test', 'test'),
  ssl_verify_peer: false
)

# rubocop:disable Metrics/BlockLength
describe 'Kinesis with Localstack', integration: true do
  before(:all) do
    # Define these variables directly in before(:all)
    @stream_name = 'test-stream'
    @table_name = 'test-table'
    @consumer_group = 'test-group'
    @consumer_id = ENV['KINESIS_CONSUMER_ID']

    # Configure AWS globally
    Aws.config.update(
      endpoint: 'http://localhost:4566',
      region: 'us-east-1',
      credentials: Aws::Credentials.new('test', 'test'),
      ssl_verify_peer: false
    )

    # Set up Kinesis stream with test data
    @kinesis_client = LocalstackHelper.kinesis_client
    @dynamodb_client = LocalstackHelper.dynamodb_client

    # Create stream with shards
    LocalstackHelper.create_test_stream(@stream_name, 2)

    # Create DynamoDB table
    LocalstackHelper.create_dynamodb_table(@table_name)

    # Put test records and save their sequence numbers
    @records = LocalstackHelper.put_test_records(@stream_name, [
                                                   'test record 1',
                                                   'test record 2',
                                                   'test record 3',
                                                   'test record 4'
                                                 ])

    # Initialize the DynamoDB state properly
    # Get current shard IDs
    shards_response = @kinesis_client.list_shards(stream_name: @stream_name)
    shard_ids = shards_response.shards.map(&:shard_id)

    # Build the shards map with proper format
    shards_map = {}
    shard_ids.each do |shard_id|
      shards_map[shard_id] = {
        'consumerId' => @consumer_id,
        'expiresIn' => (Time.now - 3600).utc.iso8601, # Expired lock
        'checkpoint' => @records.first.sequence_number, # Start after first record
        'heartbeat' => Time.now.utc.iso8601
      }
    end

    puts "Created records: #{@records}"
    puts "Shards map: #{shards_map}"
    # Store in DynamoDB
    @dynamodb_client.put_item(
      table_name: @table_name,
      item: {
        'consumerGroup' => @consumer_group,
        'streamName' => @stream_name,
        'shards' => shards_map
      }
    )

    # Allow DynamoDB to propagate
    sleep 1
  end

  it 'can read records from a stream' do
    # Create a consumer with clear configuration
    consumer = Kinesis::Consumer.new(
      stream_name: @stream_name, # Use the instance variable, not the let
      dynamodb: {
        table_name: @table_name,
        consumer_group: @consumer_group,
        client: @dynamodb_client
      },
      kinesis: {
        client: @kinesis_client
      }
    )

    # Set up a record collection mechanism
    records_received = []
    max_records = 3

    # Read records with timeout
    begin
      Timeout.timeout(10) do
        consumer.each do |record|
          puts "Received record: #{record.inspect}"
          records_received << record
          break if records_received.size >= max_records
        end
      end
    rescue Timeout::Error
      # Timeout is acceptable - we'll check what we received
    end

    # Verify we got records
    expect(records_received.size).to be > 0

    # Verify record content
    expect(records_received.first.data).to include('test record')
  end

  xit 'can use enhanced fan-out' do
    # Create a consumer with enhanced fan-out and Localstack configuration
    consumer = Kinesis::Consumer.new(
      stream_name: @stream_name,  # Use instance variable
      dynamodb: {
        table_name: @table_name,  # Use instance variable
        consumer_group: @consumer_group, # Use instance variable
        client: @dynamodb_client
      },
      kinesis: {
        client: @kinesis_client
      },
      logger: Logger.new($stdout),
      use_enhanced_fan_out: true,
      consumer_name: 'test-consumer'
    )

    # Set up a record queue to receive records
    record_queue = Queue.new
    # Set up a thread to read records
    thread = Thread.new do
      begin
        consumer.each do |record|
          puts "Received record: #{record.inspect}"
          record_queue << record
          break if record_queue.size >= 3 # Stop after getting 3 records
        end
      rescue StandardError => e
        puts "Error in thread: #{e.message}"
        puts e.backtrace.join("\n")
      end
    end

    # Wait for records to be processed
    sleep 5

    thread.join(5)

    # Check that we received records
    expect(record_queue.size).to be > 0
  end

  puts "Using consumer ID: #{ENV['KINESIS_CONSUMER_ID']}"
end
# rubocop:enable Metrics/BlockLength
