# frozen_string_literal: true

require 'rspec'
require 'spec_helper'
require 'kinesis/consumer'
require 'aws-sdk-kinesis'
require 'aws-sdk-dynamodb'

describe Kinesis::AsyncProducer do
  subject do
    producer = described_class.new(
      stream_name: 'test',
      buffer_time: 0.5,
      record_queue: Queue.new
    )
    producer.preprocess
    producer
  end

  describe '#process' do
    let(:proxy_message) do
      {
        'type' => 'touch_point_canceled',
        'data' => { 'a': '1' }
      }
    end

    it 'should work' do
      expect_any_instance_of(Kinesis::Subthread).to receive(:start)

      expect(Aws::Kinesis::Client).to receive(:new).and_return(
        Aws::Kinesis::Client.new(stub_responses: true)
      )

      expect_any_instance_of(Kinesis::AsyncProducer).to receive(:flush_records)

      expect(subject.process).to eq 0
    end
  end
end
