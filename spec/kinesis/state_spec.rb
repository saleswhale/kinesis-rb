# frozen_string_literal: true

require 'rspec'
require 'spec_helper'
require 'kinesis/state'
require 'aws-sdk-kinesis'
require 'aws-sdk-dynamodb'

describe Kinesis::State do
  let(:logger) { double(warn: nil) }
  let(:state) { described_class.new(stream_name: 'test', logger: logger) }

  describe '#get_iterator_args' do
    context 'if shard does not exist' do
      it 'should return LATEST' do
        resp = state.get_iterator_args('non_existent_shard')

        expect(resp).to eq({ shard_iterator_type: 'LATEST' })
      end
    end

    context 'if shard exists' do
      let(:shard_name) { 'test' }
      let(:iterator_args) { state.get_iterator_args(shard_name) }

      before do
        state.shards[shard_name] = {
          'checkpoint' => 'test123',
          'consumerId' => shard_name,
          'expiresIn' => (Time.now.utc + (60 * 60 * 24)).iso8601 # 24 hours from now
        }
      end

      context 'and heartbeat is up-to-date' do
        before do
          state.shards[shard_name]['heartbeat'] = Time.now.utc.iso8601
        end

        it 'should return AFTER_SEQUENCE_NUMBER' do
          expect(iterator_args).to include({ shard_iterator_type: 'AFTER_SEQUENCE_NUMBER' })
        end
      end

      context 'and heartbeat is stale' do
        before do
          state.shards[shard_name]['heartbeat'] = (Time.now.utc - (60 * 60 * 25)).iso8601 # 25 hours ago
        end

        it 'should log a warning' do
          expect(logger).to receive(:warn)
          iterator_args
        end

        it 'should return LATEST' do
          expect(iterator_args).to include({ shard_iterator_type: 'LATEST' })
        end
      end
    end
  end
end
