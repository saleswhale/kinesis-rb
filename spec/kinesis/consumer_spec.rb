# frozen_string_literal: true

require 'rspec'
require 'kinesis/consumer'

describe Kinesis::Consumer do
  subject do
    described_class.new(stream_name: 'dev-odina')
  end

  describe '#each' do
    it 'should work' do
      puts 'do something'
    end
  end
end
