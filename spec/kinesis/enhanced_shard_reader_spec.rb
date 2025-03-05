# frozen_string_literal: true

require 'rspec'
require 'spec_helper'
require 'kinesis/enhanced_shard_reader'

# rubocop:disable Metrics/BlockLength
describe Kinesis::EnhancedShardReader do
  let(:error_queue) { Queue.new }
  let(:record_queue) { Queue.new }
  let(:logger) { instance_double('Logger', info: nil, error: nil, warn: nil) }
  let(:kinesis_client) { instance_double(Aws::Kinesis::AsyncClient) }
  let(:output_stream) { instance_double(Aws::Kinesis::EventStreams::SubscribeToShardEventStream) }
  let(:async_response) { instance_double('Seahorse::Client::AsyncResponse', wait: nil) }

  before do
    # Mock the SubthreadLoop behavior
    allow_any_instance_of(Kinesis::SubthreadLoop).to receive(:run) do |instance|
      # Call process once to simulate the loop
      instance.send(:process)
    end

    # Mock the AsyncClient creation
    allow(Aws::Kinesis::AsyncClient).to receive(:new).and_return(kinesis_client)

    # Mock the EventStream creation
    allow(Aws::Kinesis::EventStreams::SubscribeToShardEventStream).to receive(:new).and_return(output_stream)

    # Mock the event handlers
    allow(output_stream).to receive(:on_subscribe_to_shard_event_event).and_yield(
      instance_double('Aws::Kinesis::Types::SubscribeToShardEvent', records: [])
    )
    allow(output_stream).to receive(:on_error_event)

    # Mock the subscribe_to_shard method
    allow(kinesis_client).to receive(:subscribe_to_shard).and_return(async_response)
  end

  subject do
    described_class.new(
      error_queue: error_queue,
      logger: logger,
      record_queue: record_queue,
      shard_id: 'test-shard-id',
      sleep_time: 0.1,
      consumer_arn: 'test-consumer-arn',
      starting_position: { type: 'LATEST' }
    )
  end

  # Helper method to ensure preprocess is called
  def prepare_subject
    # Call preprocess to set up the kinesis client
    subject.send(:preprocess)
    subject
  end

  describe '#initialize' do
    it 'sets up a subscription to the shard' do
      # Prepare the subject first
      reader = prepare_subject

      expect(kinesis_client).to receive(:subscribe_to_shard).with(
        consumer_arn: 'test-consumer-arn',
        shard_id: 'test-shard-id',
        starting_position: { type: 'LATEST' },
        output_event_stream_handler: output_stream
      )

      # Trigger the process method
      reader.send(:process)
    end
  end

  describe '#shutdown' do
    it 'closes the subscription' do
      # Create the reader and set the async_response
      reader = prepare_subject
      reader.instance_variable_set(:@async_response, async_response)

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
      # Set up the async_response
      reader = prepare_subject
      reader.instance_variable_set(:@async_response, async_response)

      # Expect wait to be called
      expect(async_response).to receive(:wait)

      # Call the method but catch the expected exception
      expect do
        reader.send(:wait_for_events)
      end.to raise_error(StandardError, /Subscription ended/)
    end
  end

  describe 'error handling' do
    before do
      # Ensure preprocess is called for each test
      prepare_subject
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
