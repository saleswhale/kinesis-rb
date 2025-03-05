# frozen_string_literal: true

require 'rspec'
require 'spec_helper'
require 'kinesis/enhanced_shard_reader'

# rubocop:disable Metrics/BlockLength
describe Kinesis::EnhancedShardReader do
  let(:error_queue) { Queue.new }
  let(:record_queue) { Queue.new }
  let(:logger) { instance_double('Logger', info: nil, error: nil, warn: nil) }
  let(:kinesis_client) { instance_double(Aws::Kinesis::Client) }
  let(:subscription) { instance_double('Aws::Kinesis::SubscribeToShardOutput', close: nil) }
  let(:event_stream) { instance_double('Aws::Kinesis::EventStream') }

  before do
    # Mock the SubthreadLoop behavior
    allow_any_instance_of(Kinesis::SubthreadLoop).to receive(:run) do |instance|
      # Call process once to simulate the loop
      instance.send(:process)
    end

    # Mock the subscription behavior
    allow(kinesis_client).to receive(:subscribe_to_shard).and_return(subscription)
    allow(subscription).to receive(:on_event_stream).and_yield(event_stream)
    allow(event_stream).to receive(:on_record_event)
    allow(event_stream).to receive(:on_error_event)

    # Mock wait to avoid the actual blocking behavior
    allow(subscription).to receive(:wait) do
      # Don't actually wait or raise in tests
    end
  end

  subject do
    described_class.new(
      error_queue: error_queue,
      logger: logger,
      record_queue: record_queue,
      shard_id: 'test-shard-id',
      sleep_time: 0.1,
      kinesis_client: kinesis_client,
      consumer_arn: 'test-consumer-arn',
      starting_position: { type: 'LATEST' }
    )
  end

  describe '#initialize' do
    it 'sets up a subscription to the shard' do
      expect(kinesis_client).to receive(:subscribe_to_shard).with(
        consumer_arn: 'test-consumer-arn',
        shard_id: 'test-shard-id',
        starting_position: { type: 'LATEST' }
      )

      # Trigger the process method
      subject.send(:process)
    end
  end

  describe '#shutdown' do
    it 'closes the subscription' do
      # Create the reader and set the subscription
      reader = subject
      reader.instance_variable_set(:@subscription, subscription)

      # Expect the subscription to be closed
      expect(subscription).to receive(:close)

      reader.shutdown
    end
  end

  describe '#process_records' do
    it 'adds records to the queue' do
      record = instance_double('Aws::Kinesis::Record')
      subject.send(:process_records, [record])

      expect(record_queue.size).to eq(1)
      expect(record_queue.pop).to eq(['test-shard-id', record])
    end
  end

  describe '#wait_for_events' do
    it 'waits for events from the subscription' do
      # Set up the subscription
      subject.instance_variable_set(:@subscription, subscription)

      # Expect wait to be called
      expect(subscription).to receive(:wait)

      # Call the method but catch the expected exception
      expect do
        subject.send(:wait_for_events)
      end.to raise_error(StandardError, /Subscription ended/)
    end
  end

  describe 'error handling' do
    before do
      subject.instance_variable_set(:@subscription, subscription)
    end

    it 'handles JSON parsing errors' do
      error = JSON::ParserError.new('unexpected token')
      allow(kinesis_client).to receive(:subscribe_to_shard).and_raise(error)

      expect(logger).to receive(:error).at_least(:once)
      expect(error_queue).to receive(:<<).with(error)

      subject.send(:process)
    end

    it 'handles AWS service errors' do
      # Create a proper AWS error with a request context
      context = Seahorse::Client::RequestContext.new(
        operation_name: 'SubscribeToShard'
      )

      # Set the response in the context
      context.http_response.status_code = 400
      context.http_response.headers['x-amzn-RequestId'] = '1234567890ABCDEF'
      context.http_response.body = StringIO.new('{"__type":"ResourceNotFoundException","message":"Resource not found"}')

      # Create the error with the context
      error = Aws::Kinesis::Errors::ResourceNotFoundException.new(
        context,
        'Resource not found'
      )

      allow(kinesis_client).to receive(:subscribe_to_shard).and_raise(error)

      expect(logger).to receive(:error).at_least(:once)
      expect(error_queue).to receive(:<<).with(error)

      subject.send(:process)
    end

    it 'handles general errors' do
      error = StandardError.new('General error')
      allow(kinesis_client).to receive(:subscribe_to_shard).and_raise(error)

      expect(logger).to receive(:error).at_least(:once)
      expect(error_queue).to receive(:<<).with(error)

      subject.send(:process)
    end
  end
end
# rubocop:enable Metrics/BlockLength
