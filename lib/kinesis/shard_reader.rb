# frozen_string_literal: true

require 'kinesis/subthread_loop'

module Kinesis
  # Kinesis::ShardReader
  class ShardReader < SubthreadLoop
    DEFAULT_SLEEP_TIME = 1.0
    MAX_SLEEP_TIME = 30.0

    def initialize(
      error_queue:,
      logger:,
      record_queue:,
      shard_id:,
      shard_iterator:,
      sleep_time: nil
    )
      @error_queue = error_queue
      @logger = logger
      @record_queue = record_queue
      @shard_id = shard_id
      @shard_iterator = shard_iterator
      @sleep_time = sleep_time || DEFAULT_SLEEP_TIME

      super
    end

    # inside thread - instance vars
    def preprocess
      @kinesis_client = Aws::Kinesis::Client.new
      @retries = 0
    end

    def process
      sleep_time = @sleep_time
      resp = @kinesis_client.get_records(shard_iterator: @shard_iterator)

      unless resp[:next_shard_iterator]
        @logger.info(
          {
            message: 'Shard has been closed',
            shard_id: @shard_id
          }
        )
        # TODO: is it possible there are records in resp here?
        return false
      end

      @shard_iterator = resp[:next_shard_iterator]
      @record_queue << [@shard_id, resp]
      @retries = 0

      sleep_time
    rescue Aws::Kinesis::Errors::ServiceError => e
      # sleep for 1 second the first loop, 1 second the next, then 2, 4, 6, 8, ..., up to a max of 30 or
      # until we complete a successful get_records call
      if sleep_time < MAX_SLEEP_TIME && Kinesis::RETRYABLE_EXCEPTIONS.include?(e.class.name)
        sleep_time = [MAX_SLEEP_TIME, @retries * 2].min
        @retries += 1

        @logger.info(
          {
            message: 'Retryable exception encountered when getting records',
            error: e,
            retry_count: @retries
          }
        )
      else
        @logger.info(
          {
            message: 'Error encountered when getting records',
            error: e,
            shard_iterator: @shard_iterator
          }
        )
        raise e
      end

      sleep_time
    end

    def shutdown
      thread&.exit
    end
  end
end
