# frozen_string_literal: true

require 'rspec'
require 'spec_helper'
require 'kinesis/enhanced_shard_reader'

describe Kinesis::EnhancedShardReader do
  let(:error_queue) { Queue.new }
  let(:record_queue) { Queue.new }
  let(:logger) { double('Logger', info: nil, error: nil) }
  let(:kinesis_client) { instance_double(Aws::Kinesis::Client) }
  let(:subscription) { double('Subscription', close: nil) }
  let(:event_stream) { double('EventStream') }
  let(:thread) { double('Thread', alive?: true, kill: nil) }

  before do
    # Mock Thread.new to both execute the block and return our mock thread
    allow(Thread).to receive(:new) do |&block|
      block&.call
      thread
    end

    # Mock the subscription behavior
    allow(kinesis_client).to receive(:subscribe_to_shard).and_return(subscription)
    allow(subscription).to receive(:on_event_stream).and_yield(event_stream)
    allow(event_stream).to receive(:on_record_event)
    allow(event_stream).to receive(:on_error_event)
    allow(subscription).to receive(:wait)
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

      subject
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
      record = double('Record')
      subject.send(:process_records, [record])

      expect(record_queue.size).to eq(1)
      expect(record_queue.pop).to eq(['test-shard-id', record])
    end
  end
end
