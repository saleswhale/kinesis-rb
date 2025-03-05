# frozen_string_literal: true

require 'rspec'
require 'spec_helper'
require 'kinesis/consumer'
require 'aws-sdk-kinesis'

# rubocop:disable Metrics/BlockLength
describe Kinesis::Consumer, 'with Enhanced Fan-Out', integration: true do
  let(:stream_arn) { 'arn:aws:kinesis:us-east-1:123456789012:stream/test-stream' }
  let(:consumer_arn) { "#{stream_arn}/consumer/test-consumer:1234567890" }

  let(:kinesis_client) do
    client = Aws::Kinesis::Client.new(stub_responses: true)

    # Stub describe_stream with all required parameters
    client.stub_responses(:describe_stream, {
                            stream_description: {
                              stream_name: 'test-stream',
                              stream_arn: stream_arn,
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
    client.stub_responses(:describe_stream_consumer, {
                            consumer_description: {
                              consumer_name: 'test-consumer',
                              consumer_arn: consumer_arn,
                              consumer_status: 'ACTIVE',
                              consumer_creation_timestamp: Time.now,
                              stream_arn: stream_arn
                            }
                          })

    # For subscribe_to_shard, we need to mock the event stream differently
    # Instead of stubbing it directly, we'll mock the method call
    allow(client).to receive(:subscribe_to_shard).and_return(
      instance_double('Aws::Kinesis::SubscribeToShardOutput',
                      event_stream: instance_double('Aws::Kinesis::EventStream'),
                      on_event_stream: nil,
                      wait: nil,
                      close: nil)
    )

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

  let(:state) do
    instance_double(Kinesis::State,
                    lock_shard: true,
                    get_iterator_args: { shard_iterator_type: 'LATEST' })
  end

  subject do
    consumer = described_class.new(
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

    # Set up the consumer for testing
    consumer.instance_variable_set(:@stream_info, {
                                     stream_description: {
                                       stream_arn: stream_arn,
                                       retention_period_hours: 24
                                     }
                                   })
    consumer.instance_variable_set(:@state, state)

    # Set the consumer ARN for tests that don't call register_consumer
    consumer.instance_variable_set(:@consumer_arn, consumer_arn)

    consumer
  end

  before do
    # Mock EnhancedShardReader to prevent actual thread creation
    enhanced_reader = instance_double(Kinesis::EnhancedShardReader)
    allow(enhanced_reader).to receive(:alive?).and_return(true)
    allow(enhanced_reader).to receive(:shutdown)
    allow(Kinesis::EnhancedShardReader).to receive(:new).and_return(enhanced_reader)
  end

  it 'registers a consumer when initialized' do
    # We need to test the register_consumer method directly
    expect(kinesis_client).to receive(:describe_stream_consumer).with(
      stream_arn: stream_arn,
      consumer_name: 'test-consumer'
    )
    subject.send(:register_consumer)
  end

  it 'uses EnhancedShardReader for shards' do
    # We need to verify that start_enhanced_shard_reader is called
    expect(subject).to receive(:start_enhanced_shard_reader)
    subject.send(:setup_shards)
  end

  context 'when consumer does not exist' do
    before do
      # First call raises ResourceNotFoundException, second call succeeds
      allow(kinesis_client).to receive(:describe_stream_consumer)
        .and_raise(Aws::Kinesis::Errors::ResourceNotFoundException.new(nil, 'Not found'))
        .once

      allow(kinesis_client).to receive(:register_stream_consumer).and_return(
        instance_double('Aws::Kinesis::Types::RegisterStreamConsumerOutput',
                        consumer: instance_double('Aws::Kinesis::Types::Consumer',
                                                  consumer_name: 'test-consumer',
                                                  consumer_arn: consumer_arn))
      )
    end

    it 'creates a new consumer' do
      expect(kinesis_client).to receive(:register_stream_consumer).with(
        stream_arn: stream_arn,
        consumer_name: 'test-consumer'
      )
      subject.send(:register_consumer)
    end

    it 'stores the consumer ARN from the response' do
      subject.instance_variable_set(:@consumer_arn, nil)
      subject.send(:register_consumer)
      expect(subject.send(:consumer_arn)).to eq(consumer_arn)
    end
  end

  describe '#consumer_arn' do
    it 'returns the stored consumer ARN' do
      expect(subject.send(:consumer_arn)).to eq(consumer_arn)
    end

    it 'raises an error if consumer ARN is not available' do
      subject.instance_variable_set(:@consumer_arn, nil)
      expect { subject.send(:consumer_arn) }.to raise_error(/Consumer ARN not available/)
    end
  end
end
# rubocop:enable Metrics/BlockLength
