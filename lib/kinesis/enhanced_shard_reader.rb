# frozen_string_literal: true

module Kinesis
  # Reads records from a Kinesis shard using Enhanced Fan-Out
  class EnhancedShardReader < SubthreadLoop
    DEFAULT_SLEEP_TIME = 1.0

    def initialize(
      error_queue:,
      logger:,
      record_queue:,
      shard_id:,
      kinesis_client:, consumer_arn:, starting_position:, sleep_time: nil
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

      super(nil) # Call parent initializer
    end

    def shutdown
      # Use the same approach as ShardReader
      thread&.exit

      return unless @subscription

      # Close subscription if it exists
      begin
        @subscription.close
      rescue StandardError => e
        @logger.error("Error closing subscription: #{e.message}")
      end
    end

    private

    def preprocess
      # No preprocessing needed
    end

    def process
      read_with_enhanced_fan_out
      @sleep_time # Return sleep time for the loop
    end

    def read_with_enhanced_fan_out
      log_start_info

      begin
        create_subscription
        setup_event_handlers
        wait_for_events
      rescue JSON::ParserError => e
        handle_json_error(e)
      rescue Aws::Errors::ServiceError => e
        handle_aws_error(e)
      rescue StandardError => e
        handle_general_error(e)
      end

      @sleep_time # Return sleep time for retry
    end

    def log_start_info
      @logger.info("Starting enhanced fan-out for shard #{@shard_id} with consumer ARN: #{@consumer_arn}")
      @logger.info("Starting position: #{@starting_position.inspect}")
    end

    def create_subscription
      @logger.warn(
        "Creating subscription for shard #{@shard_id}, " \
        "consumer ARN: #{@consumer_arn}, " \
        "starting position: #{@starting_position.inspect}"
      )
      @subscription = @kinesis_client.subscribe_to_shard(
        consumer_arn: @consumer_arn,
        shard_id: @shard_id,
        starting_position: @starting_position
      )

      @logger.info("Successfully created subscription for shard #{@shard_id}")
    end

    def setup_event_handlers
      @subscription.on_event_stream do |event_stream|
        @logger.info("Setting up event handlers for shard #{@shard_id}")

        event_stream.on_record_event do |event|
          process_records(event.records)
        end

        event_stream.on_error_event do |event|
          error_message = "Error in enhanced fan-out subscription for shard #{@shard_id}: #{event.error.message}"
          @logger.error(error_message)
          @error_queue << event.error
        end
      end
    end

    def wait_for_events
      @logger.info("Waiting for events on shard #{@shard_id}")
      @subscription.wait

      # After subscription ends, we need to raise an error to trigger a retry
      @logger.warn("Subscription ended for shard #{@shard_id}, will retry")
      raise StandardError, "Subscription ended for shard #{@shard_id}, will retry"
    end

    def handle_json_error(error)
      error_message = "JSON parsing error in enhanced fan-out for shard #{@shard_id}: #{error.message}"
      @logger.error(error_message)
      @logger.error("Consumer ARN: #{@consumer_arn}")
      @logger.error("Starting position: #{@starting_position.inspect}")
      @error_queue << error
    end

    def handle_aws_error(error)
      error_message = "AWS service error in enhanced fan-out for shard #{@shard_id}: #{error.message} (#{error.class})"
      @logger.error(error_message)

      # Log additional details if available
      error_details = []
      error_details << "Code: #{error.code}" if error.respond_to?(:code)
      if error.respond_to?(:context) && error.context.respond_to?(:request_id)
        error_details << "Request ID: #{error.context.request_id}"
      end

      @logger.error("Error details: #{error_details.join(', ')}") unless error_details.empty?
      @error_queue << error
    end

    def handle_general_error(error)
      error_message = "Error setting up enhanced fan-out for shard #{@shard_id}: #{error.message} (#{error.class})"
      @logger.error(error_message)
      @logger.error(error.backtrace.join("\n")) if error.backtrace
      @error_queue << error
    end

    def process_records(records)
      @logger.info("Processing #{records.size} records from shard #{@shard_id}")

      records.each do |record|
        @record_queue << [@shard_id, record]
      end
    end
  end
end
