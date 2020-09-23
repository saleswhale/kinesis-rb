# frozen_string_literal: true

require 'rspec'
require 'spec_helper'
require 'kinesis/producer'
require 'aws-sdk-kinesis'

describe Kinesis::Producer do
  subject do
    described_class.new(stream_name: 'test')
  end

  describe '#put' do
    let(:message) do
      {
        'type' => 'test',
        'data' => { 'a': '1' }
      }
    end

    it 'should work' do
      expect_any_instance_of(Kinesis::AsyncProducer).to receive(:start)

      subject.put(message)
    end
  end
end
