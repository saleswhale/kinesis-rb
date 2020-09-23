# frozen_string_literal: true

require 'rspec'
require 'spec_helper'
require 'kinesis/consumer'
require 'aws-sdk-kinesis'
require 'aws-sdk-dynamodb'

describe Kinesis::Consumer do
  subject do
    described_class.new(stream_name: 'test')
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
  end
end
