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
      consumer_arn:,
      starting_position:,
      sleep_time: nil
    )
      @error_queue = error_queue
      @logger = logger
      @record_queue = record_queue
      @shard_id = shard_id
      @sleep_time = sleep_time || DEFAULT_SLEEP_TIME
      @consumer_arn = consumer_arn
      @starting_position = starting_position
      @subscription = nil
      @async_response = nil

      super(nil) # Call parent initializer
    end

    def shutdown
      # Use the same approach as ShardReader
      thread&.exit

      return unless @async_response

      # No need to close the async_response, it doesn't have a close method
      # Just log that we're shutting down
      @logger.info("Shutting down enhanced fan-out for shard #{@shard_id}")
    end

    private

    def preprocess
      # Create a fresh Kinesis AsyncClient for HTTP/2 streaming
      @kinesis_client = Aws::Kinesis::AsyncClient.new
    end

    def process
      read_with_enhanced_fan_out
      @sleep_time # Return sleep time for the loop
    end

    def read_with_enhanced_fan_out
      log_start_info

      begin
        create_subscription
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
      @logger.info(
        "Creating subscription for shard #{@shard_id}, " \
        "consumer ARN: #{@consumer_arn}, " \
        "starting position: #{@starting_position.inspect}"
      )

      # Create an event stream handler
      output_stream = Aws::Kinesis::EventStreams::SubscribeToShardEventStream.new

      # Set up event handlers
      output_stream.on_subscribe_to_shard_event_event do |event|
        process_records(event.records) if event.records && !event.records.empty?
      end

      output_stream.on_error_event do |error_event|
        error_message = "Error in enhanced fan-out subscription for shard #{@shard_id}: #{error_event.error.message}"
        @logger.error(error_message)
        @error_queue << error_event.error
      end

      # Subscribe to the shard with the event stream handler
      @async_response = @kinesis_client.subscribe_to_shard(
        consumer_arn: @consumer_arn,
        shard_id: @shard_id,
        starting_position: @starting_position,
        output_event_stream_handler: output_stream
      )

      @logger.info("Successfully created subscription for shard #{@shard_id}")
    end

    def wait_for_events
      @logger.info("Waiting for events on shard #{@shard_id}")

      # Wait for the async response to complete
      @async_response.wait

      # After subscription ends, log it and return
      # The next iteration of the loop will create a new subscription
      @logger.info("Subscription ended for shard #{@shard_id}, will renew")
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
      # Check if this is a known HTTP/2 stream initialization error
      if error.is_a?(Seahorse::Client::Http2StreamInitializeError)
        error_message = "HTTP/2 stream initialization error for shard #{@shard_id}: #{error.message}"
        @logger.warn(error_message)
        @logger.warn('Will retry the subscription')
        # Add to error queue so it can be tracked, but it will be retried
      else
        # This is an unexpected error, log it as such
        error_message = "Error setting up enhanced fan-out for shard #{@shard_id}: #{error.message} (#{error.class})"
        @logger.error(error_message)
        @logger.error(error.backtrace.join("\n")) if error.backtrace
      end
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
