# frozen_string_literal: true

require 'timeout'
require 'kinesis/enhanced_shard_reader/error_handling'

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
    include Kinesis::EnhancedShardReaderErrorHandling

    DEFAULT_SLEEP_TIME = 1.0
    DEFAULT_WAIT_TIMEOUT = 360 # 6 minutes timeout (1 minute buffer as AWS naturally has a 5 minute timeout)

    # Initialize a new Enhanced Fan-Out shard reader
    #
    # @param error_queue [Queue] Queue to push errors to for monitoring
    # @param logger [Logger] Logger instance for logging operations and errors
    # @param record_queue [Queue] Queue to push received records to for processing
    # @param shard_id [String] ID of the Kinesis shard to read from
    # @param consumer_arn [String] ARN of the registered consumer for Enhanced Fan-Out
    # @param starting_position [Hash] Position to start reading from (e.g., LATEST, TRIM_HORIZON, etc.)
    # @param sleep_time [Float, nil] Time to sleep between retry attempts
    # @param wait_timeout [Float, nil] Timeout for waiting for events
    def initialize(
      error_queue:,
      logger:,
      record_queue:,
      shard_id:,
      consumer_arn:,
      starting_position:,
      sleep_time: nil,
      wait_timeout: nil
    )
      @error_queue = error_queue
      @logger = logger
      @record_queue = record_queue
      @shard_id = shard_id
      @sleep_time = sleep_time || DEFAULT_SLEEP_TIME
      @wait_timeout = wait_timeout || DEFAULT_WAIT_TIMEOUT
      @consumer_arn = consumer_arn
      @starting_position = starting_position
      @subscription = nil
      @async_response = nil
      @continuation_sequence_number = nil
      @continuation_mutex = Mutex.new

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

    # Add this accessor
    attr_reader :continuation_sequence_number

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

        # Update the starting position for next iteration
        @starting_position = next_starting_position
      rescue JSON::ParserError, Aws::Errors::ServiceError => e
        handle_error(e)
      rescue StandardError => e
        # Handle separately to avoid shadowing
        handle_error(e)
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

      # Set up event handlers with continuation tracking
      output_stream.on_subscribe_to_shard_event_event do |event|
        # If the continuation sequence number has changed, log it
        if event.continuation_sequence_number &&
           @continuation_sequence_number != event.continuation_sequence_number
          @logger.info('Updated continuation sequence number: ' \
                      "#{@continuation_sequence_number} -> #{event.continuation_sequence_number}")
        end

        @continuation_mutex.synchronize do
          @continuation_sequence_number = event.continuation_sequence_number
        end

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
    # This method blocks until the subscription ends, an error occurs, or timeout is reached
    def wait_for_events
      @logger.info("Waiting for events on shard #{@shard_id} with timeout of #{@wait_timeout} seconds")

      begin
        # Wrap the wait call with a timeout to prevent stuck connections
        Timeout.timeout(@wait_timeout) do
          @async_response.wait
        end

        @logger.info("Subscription ended normally for shard #{@shard_id}, will renew")
      rescue Timeout::Error
        @logger.warn("Timeout after #{@wait_timeout} seconds waiting for events on shard #{@shard_id}")
        @logger.warn('HTTP/2 connection may be stuck, forcing reconnection')
        # Don't add timeout to error queue as this is a handled case
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

    # Add method to get starting position for next subscription
    def next_starting_position
      current_continuation = @continuation_mutex.synchronize do
        @continuation_sequence_number
      end

      if current_continuation
        @logger.info("Using continuation sequence number: #{current_continuation}")
        {
          type: 'AFTER_SEQUENCE_NUMBER',
          sequence_number: current_continuation
        }
      else
        @starting_position
      end
    end
  end
end
