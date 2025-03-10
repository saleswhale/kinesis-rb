# frozen_string_literal: true

module Kinesis
  # EnhancedShardReader is responsible for consuming records from a Kinesis shard
  # using Enhanced Fan-Out (EFO). EFO provides dedicated throughput of 2MB/second per
  # consumer per shard and uses HTTP/2 for push-based streaming of records.
  #
  # This class handles:
  # - Establishing and maintaining HTTP/2 streaming connections to Kinesis
  # - Processing records as they arrive via the streaming connection
  # - Error handling and automatic retry/reconnection
  # - Graceful shutdown of streaming connections
  class EnhancedShardReader < SubthreadLoop
    DEFAULT_SLEEP_TIME = 1.0

    # Initialize a new Enhanced Fan-Out shard reader
    #
    # @param error_queue [Queue] Queue to push errors to for monitoring
    # @param logger [Logger] Logger instance for logging operations and errors
    # @param record_queue [Queue] Queue to push received records to for processing
    # @param shard_id [String] ID of the Kinesis shard to read from
    # @param consumer_arn [String] ARN of the registered consumer for Enhanced Fan-Out
    # @param starting_position [Hash] Position to start reading from (e.g., LATEST, TRIM_HORIZON, etc.)
    # @param sleep_time [Float, nil] Time to sleep between retry attempts
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

    # Shutdown the shard reader and clean up resources
    # This method is called when the consumer is shutting down or
    # when a shard has been fully processed
    def shutdown
      # Use the same approach as ShardReader
      thread&.exit

      return unless @async_response

      # No need to close the async_response, it doesn't have a close method
      # Just log that we're shutting down
      @logger.info("Shutting down enhanced fan-out for shard #{@shard_id}")
    end

    private

    # Prepare resources needed before starting the main processing loop
    # Creates a new AsyncClient for HTTP/2 streaming connections
    def preprocess
      # Create a fresh Kinesis AsyncClient for HTTP/2 streaming
      @kinesis_client = Aws::Kinesis::AsyncClient.new
    end

    # Main processing method called by the SubthreadLoop parent class
    # Returns the sleep time for the next iteration
    #
    # @return [Float] Sleep time for the next iteration
    def process
      read_with_enhanced_fan_out
      @sleep_time # Return sleep time for the loop
    end

    # Main method for reading records using Enhanced Fan-Out
    # Sets up a subscription to the shard and processes records as they arrive
    # Handles errors and performs cleanup after subscription ends or fails
    #
    # @return [Float] Sleep time for retry in case of failure
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
      ensure
        cleanup_connection
      end

      @sleep_time # Return sleep time for retry
    end

    # Log information about starting the Enhanced Fan-Out reader
    def log_start_info
      @logger.info("Starting enhanced fan-out for shard #{@shard_id} with consumer ARN: #{@consumer_arn}")
      @logger.info("Starting position: #{@starting_position.inspect}")
    end

    # Create a subscription to the Kinesis shard using Enhanced Fan-Out
    # Sets up event handlers for processing records and handling errors
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

    # Wait for and process events from the subscription
    # This method blocks until the subscription ends or an error occurs
    def wait_for_events
      @logger.info("Waiting for events on shard #{@shard_id}")

      begin
        # Use the standard wait method that's likely mocked in your tests
        @async_response.wait

        @logger.info("Subscription ended normally for shard #{@shard_id}, will renew")
      rescue Seahorse::Client::Http2StreamInitializeError => e
        # This is the specific error you're seeing
        @logger.warn("HTTP/2 stream reset during wait for shard #{@shard_id}: #{e.message}")
        @logger.warn('This is normal after connection expiration, will establish a new subscription')
        # Sleep briefly before attempting to reconnect
        sleep(1)
      rescue StandardError => e
        @logger.warn("Error waiting for events on shard #{@shard_id}: #{e.class} - #{e.message}")
        # Don't add this to error queue, as we'll retry
      end
    end

    # Clean up resources after a subscription ends or fails
    # Closes connections and recreates clients as needed
    def cleanup_connection
      # Release any resources
      if @async_response
        begin
          # Try to explicitly close the connection if possible
          @async_response.stream.close if @async_response.respond_to?(:stream) &&
                                          @async_response.stream.respond_to?(:close)
        rescue StandardError => e
          @logger.warn("Error while closing async response: #{e.message}")
        end
      end

      @async_response = nil
      @subscription = nil

      # Also recreate the Kinesis client for a fresh connection
      @kinesis_client = Aws::Kinesis::AsyncClient.new
    end

    # Check if the current thread is alive and not marked for shutdown
    #
    # @return [Boolean] True if the thread is alive and not marked for shutdown
    def thread_alive?
      # Helper method to check if the current thread is still alive
      # This helps avoid deadlocks if the parent thread is shutting down
      Thread.current.alive? && !Thread.current[:shutdown]
    end

    # Handle JSON parsing errors that may occur when processing records
    #
    # @param error [JSON::ParserError] The JSON parsing error
    def handle_json_error(error)
      error_message = "JSON parsing error in enhanced fan-out for shard #{@shard_id}: #{error.message}"
      @logger.error(error_message)
      @logger.error("Consumer ARN: #{@consumer_arn}")
      @logger.error("Starting position: #{@starting_position.inspect}")
      @error_queue << error
    end

    # Handle AWS service errors that may occur when interacting with Kinesis
    #
    # @param error [Aws::Errors::ServiceError] The AWS service error
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

    # Handle general errors that may occur during enhanced fan-out operations
    #
    # @param error [StandardError] The error that occurred
    def handle_general_error(error)
      # Check if this is a known HTTP/2 stream initialization error
      if error.is_a?(Seahorse::Client::Http2StreamInitializeError)
        error_message = "HTTP/2 stream initialization error for shard #{@shard_id}: " \
                         "#{error.message} - #{error.original_error.message}"
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

    # Process records received from the Kinesis stream
    # Adds each record to the record queue for further processing
    #
    # @param records [Array<Aws::Kinesis::Types::Record>] Records received from Kinesis
    def process_records(records)
      @logger.info("Processing #{records.size} records from shard #{@shard_id}")

      records.each do |record|
        @record_queue << [@shard_id, record]
      end
    end
  end
end
