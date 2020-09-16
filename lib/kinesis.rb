# frozen_string_literal: true

require 'kinesis/version'

module Kinesis
  RETRY_EXCEPTIONS = %w[
    ProvisionedThroughputExceededException
    ThrottlingException
  ].freeze
end
