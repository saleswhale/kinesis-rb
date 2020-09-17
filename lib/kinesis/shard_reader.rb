# frozen_string_literal: true

module Kinesis
  # Kinesis::ShardReader
  class ShardReader < SubthreadLoop
    DEFAULT_SLEEP_TIME = 1.0
    MAX_SLEEP_TIME = 30.0
    RETRYABLE_EXCEPTIONS = %w[
      ProvisionedThroughputExceededException
      ThrottlingException
    ].freeze

    def initialize(shard_id:, shard_iterator:, record_queue:, error_queue:, sleep_time: nil)
      @error_queue = error_queue
      @record_queue = record_queue
      @shard_id = shard_id
      @shard_iterator = shard_iterator
      @sleep_time = sleep_time || DEFAULT_SLEEP_TIME
    end

    # inside thread - instance vars
    def preprocess
      @kinesis_client = Aws::Kinesis::Client.new
      @retries = 0
    end

    def process
      sleep_time = @sleep_time
      resp = @kinesis_client.get_records(shard_iterator: @shard_iterator)

      # Log: the shard has been closed
      return false unless resp['NextShardIterator']

      @shard_iterator = resp['NextShardIterator']
      @record_queue << [@shard_id, resp]
      @retries = 0

      sleep_time
    rescue StandardError => e
      # sleep for 1 second the first loop, 1 second the next, then 2, 4, 6, 8, ..., up to a max of 30 or
      # until we complete a successful get_records call
      if sleep_time < MAX_SLEEP_TIME && RETRYABLE_EXCEPTIONS.include?(e.class.name)
        sleep_time = [MAX_SLEEP_TIME, @retries * 2].min
        @retries += 1
      else
        # Log: which error
        sleep_time = false
      end

      sleep_time
    end
  end
end
