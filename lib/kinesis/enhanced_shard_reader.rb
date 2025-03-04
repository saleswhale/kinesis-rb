# frozen_string_literal: true

module Kinesis
  # Reads records from a Kinesis shard using Enhanced Fan-Out
  class EnhancedShardReader
    DEFAULT_SLEEP_TIME = 1.0

    def initialize(
      error_queue:,
      logger:,
      record_queue:,
      shard_id:,
      sleep_time: nil,
      kinesis_client:,
      consumer_arn:,
      starting_position:
    )
      @error_queue = error_queue
      @logger = logger
      @record_queue = record_queue
      @shard_id = shard_id
      @sleep_time = sleep_time || DEFAULT_SLEEP_TIME
      @kinesis_client = kinesis_client
      @consumer_arn = consumer_arn
      @starting_position = starting_position
      @subscription = nil
      
      @thread = Thread.new do
        read_with_enhanced_fan_out
      end
    end

    def alive?
      @thread.alive?
    end

    def shutdown
      @thread.kill if @thread.alive?
      
      # Close subscription if it exists
      if @subscription
        begin
          @subscription.close
        rescue StandardError => e
          @logger.error("Error closing subscription: #{e.message}")
        end
      end
    end

    private

    def read_with_enhanced_fan_out
      begin
        @subscription = @kinesis_client.subscribe_to_shard(
          consumer_arn: @consumer_arn,
          shard_id: @shard_id,
          starting_position: @starting_position
        )

        @subscription.on_event_stream do |event_stream|
          event_stream.on_record_event do |event|
            process_records(event.records)
          end

          event_stream.on_error_event do |event|
            @error_queue << event.error
            @logger.error("Error in enhanced fan-out subscription for shard #{@shard_id}: #{event.error.message}")
          end
        end

        # This will block until the subscription is closed
        @subscription.wait
      rescue StandardError => e
        @error_queue << e
        @logger.error("Error setting up enhanced fan-out for shard #{@shard_id}: #{e.message}")
        sleep @sleep_time
        retry
      end
    end

    def process_records(records)
      records.each do |record|
        @record_queue << [@shard_id, record]
      end
    end
  end
end 