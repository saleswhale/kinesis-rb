# frozen_string_literal: true

require 'aws-sdk-kinesis'

require 'kinesis/version'
require 'kinesis/consumer'
require 'kinesis/producer'

module Kinesis
  RETRYABLE_EXCEPTIONS = %w[
    Aws::Kinesis::Errors::ProvisionedThroughputExceededException
    Seahorse::Client::Http2StreamInitializeError
  ].freeze
end
