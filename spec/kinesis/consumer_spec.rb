# frozen_string_literal: true

require 'rspec'
require 'spec_helper'
require 'kinesis/consumer'
require 'aws-sdk'

describe Kinesis::Consumer do
  let(:kinesis_client) do
    client = Aws::Kinesis::Client.new(stub_responses: true)
    client.stub_responses(:describe_stream, {
      stream_description: {
        stream_name: 'dummy',
        stream_arn: 'dummy',
        stream_status: 'dummy',
        has_more_shards: false,
        retention_period_hours: 1,
        stream_creation_timestamp: Time.now,
        enhanced_monitoring: [],
        shards: [{
          hash_key_range: {
            starting_hash_key: '123',
            ending_hash_key: '123'
          },
          sequence_number_range: {
            starting_sequence_number: '123'
          },
          shard_id: 'dummy_shard_id'
        }]
      }
    })
    client
  end

  subject do
    described_class.new(
      stream_name: 'test',
      kinesis: { client: kinesis_client }
    )
  end

  describe '#each' do
    let(:proxy_message) do
      {
        'type' => 'touch_point_sent',
        'data' => { 'a': '1' }
      }
    end

    it 'should work' do
      allow_any_instance_of(Kinesis::Consumer).to receive(:each).and_yield(proxy_message)

      message = nil

      subject.each { |m| message = m }

      expect(message['type']).to eq(proxy_message['type'])
    end

    it 'should handle SIGINT' do
      expect_any_instance_of(Kinesis::ShardReader).to receive(:shutdown)
      Thread.new { sleep 0.1; Process.kill('INT', Process.pid) }
      expect { subject.each {} }.not_to raise_error
    end

    it 'should handle SIGTERM' do
      expect_any_instance_of(Kinesis::ShardReader).to receive(:shutdown)
      Thread.new { sleep 0.1; Process.kill('TERM', Process.pid) }
      expect { subject.each {} }.not_to raise_error
    end

    it 'should not handle other signals' do
      expect_any_instance_of(Kinesis::ShardReader).to receive(:shutdown)
      Thread.new { sleep 0.1; Process.kill('USR1', Process.pid) }
      expect { subject.each {} }.to raise_error(SignalException)
    end
  end
end
