# frozen_string_literal: true

require 'rspec'
require 'spec_helper'
require 'kinesis/consumer'

# rubocop:disable Metrics/BlockLength
describe 'Kinesis::Consumer with Enhanced Fan-Out', integration: true do
  let(:kinesis_client) do
    client = Aws::Kinesis::Client.new(stub_responses: true)

    # Stub describe_stream with all required parameters
    client.stub_responses(:describe_stream, {
                            stream_description: {
                              stream_name: 'test-stream',
                              stream_arn: 'arn:aws:kinesis:us-east-1:123456789012:stream/test-stream',
                              stream_status: 'ACTIVE',
                              stream_creation_timestamp: Time.now,
                              retention_period_hours: 24,
                              shards: [{
                                shard_id: 'shardId-000000000000',
                                hash_key_range: {
                                  starting_hash_key: '0',
                                  ending_hash_key: '340282366920938463463374607431768211455'
                                },
                                sequence_number_range: {
                                  starting_sequence_number: '49590338271490256608559692538361571095921575989136588898'
                                }
                              }],
                              has_more_shards: false,
                              enhanced_monitoring: []
                            }
                          })

    # Stub list_shards
    client.stub_responses(:list_shards, {
                            shards: [{
                              shard_id: 'shardId-000000000000',
                              hash_key_range: {
                                starting_hash_key: '0',
                                ending_hash_key: '340282366920938463463374607431768211455'
                              },
                              sequence_number_range: {
                                starting_sequence_number: '49590338271490256608559692538361571095921575989136588898'
                              }
                            }]
                          })

    # Stub describe_stream_consumer (existing consumer)
    consumer_arn = 'arn:aws:kinesis:us-east-1:123456789012:stream/test-stream/consumer/test-consumer'
    client.stub_responses(:describe_stream_consumer, {
                            consumer_description: {
                              consumer_name: 'test-consumer',
                              consumer_arn: consumer_arn,
                              consumer_status: 'ACTIVE',
                              consumer_creation_timestamp: Time.now
                            }
                          })

    # Stub subscribe_to_shard
    client.stub_responses(:subscribe_to_shard, {})

    # Stub register_stream_consumer
    client.stub_responses(:register_stream_consumer, {
                            consumer: {
                              consumer_name: 'test-consumer',
                              consumer_arn: consumer_arn,
                              consumer_status: 'CREATING',
                              consumer_creation_timestamp: Time.now
                            }
                          })

    client
  end

  let(:dynamodb_client) do
    Aws::DynamoDB::Client.new(stub_responses: true)
  end

  subject do
    described_class.new(
      stream_name: 'test-stream',
      dynamodb: {
        client: dynamodb_client,
        table_name: 'test-table',
        consumer_group: 'test-group'
      },
      kinesis: {
        client: kinesis_client
      },
      use_enhanced_fan_out: true,
      consumer_name: 'test-consumer'
    )
  end

  before do
    # Mock EnhancedShardReader to prevent actual thread creation
    enhanced_reader = instance_double(Kinesis::EnhancedShardReader)
    allow(enhanced_reader).to receive(:alive?).and_return(true)
    allow(enhanced_reader).to receive(:shutdown)
    allow(Kinesis::EnhancedShardReader).to receive(:new).and_return(enhanced_reader)

    # Mock State to avoid DynamoDB interactions
    allow_any_instance_of(Kinesis::State).to receive(:lock_shard).and_return(true)
    allow_any_instance_of(Kinesis::State).to receive(:get_iterator_args).and_return({ shard_iterator_type: 'LATEST' })
  end

  it 'registers a consumer when initialized' do
    expect(kinesis_client).to receive(:describe_stream_consumer)
    subject
  end

  it 'uses EnhancedShardReader for shards' do
    expect(Kinesis::EnhancedShardReader).to receive(:new)
    subject.send(:setup_shards)
  end

  context 'when consumer does not exist' do
    before do
      # First call raises ResourceNotFoundException, second call succeeds
      allow(kinesis_client).to receive(:describe_stream_consumer)
        .and_raise(Aws::Kinesis::Errors::ResourceNotFoundException.new(nil, 'Not found'))
        .once

      allow(kinesis_client).to receive(:register_stream_consumer).and_return(
        double('RegisterResponse', consumer: double('Consumer', consumer_name: 'test-consumer'))
      )
    end

    it 'creates a new consumer' do
      expect(kinesis_client).to receive(:register_stream_consumer)
      subject
    end
  end
end
# rubocop:enable Metrics/BlockLength
