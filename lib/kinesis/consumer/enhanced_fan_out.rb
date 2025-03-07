# frozen_string_literal: true

require 'logger'

module Kinesis
  class Consumer
    # The EnhancedFanOut module provides functionality for using Kinesis Enhanced Fan-Out (EFO)
    # with the Consumer class. Enhanced Fan-Out provides dedicated throughput of 2MB/second
    # per consumer per shard, eliminating contention between consumers.
    #
    # This module handles:
    # - Registering consumers with Kinesis for Enhanced Fan-Out
    # - Managing consumer ARNs and metadata
    # - Starting Enhanced Shard Readers for consuming data via HTTP/2 streaming
    module EnhancedFanOut
      # Register the consumer with Kinesis for Enhanced Fan-Out
      # This method checks if a consumer already exists and creates a new one if needed
      # It's automatically called during Consumer initialization when use_enhanced_fan_out is true
      #
      # @return [nil] Returns nil if Enhanced Fan-Out is not enabled
      def register_consumer
        return unless @use_enhanced_fan_out

        begin
          # Check if consumer already exists using stream_arn and consumer_name
          @logger.info("Checking if consumer '#{@consumer_name}' already exists")

          # First try to describe the consumer directly (faster and what the tests expect)
          begin
            response = @kinesis_client.describe_stream_consumer(
              stream_arn: stream_arn,
              consumer_name: @consumer_name
            )
            @consumer_arn = response.consumer_description.consumer_arn
            @logger.info("Consumer '#{@consumer_name}' already exists with ARN: #{@consumer_arn}")
          rescue Aws::Kinesis::Errors::ResourceNotFoundException
            # Register a new consumer if it doesn't exist
            @logger.info("Registering new consumer '#{@consumer_name}'")

            response = @kinesis_client.register_stream_consumer(
              stream_arn: stream_arn,
              consumer_name: @consumer_name
            )

            @consumer_arn = response.consumer.consumer_arn
            @logger.info("Successfully registered consumer with ARN: #{@consumer_arn}")
          end
        rescue StandardError => e
          @logger.error("Error registering consumer: #{e.message}")
          raise
        end
      end

      # Get the ARN for the registered consumer
      # This is used when starting Enhanced Shard Readers
      #
      # @return [String] The ARN of the registered consumer
      # @raise [RuntimeError] if consumer ARN is not available
      def consumer_arn
        @consumer_arn || raise('Consumer ARN not available. Make sure register_consumer is called first.')
      end

      # Start an Enhanced Shard Reader for the specified shard
      # This creates a new EnhancedShardReader instance that uses HTTP/2 streaming
      # to receive records directly from Kinesis for the registered consumer
      #
      # @param shard_id [String] The ID of the shard to read from
      def start_enhanced_shard_reader(shard_id)
        iterator_args = @state.get_iterator_args(shard_id)
        starting_position = {
          type: iterator_args[:shard_iterator_type]
        }

        if iterator_args[:shard_iterator_type] == 'AFTER_SEQUENCE_NUMBER'
          starting_position[:sequence_number] = iterator_args[:starting_sequence_number]
        end

        @logger.info("Starting enhanced fan-out reader for shard #{shard_id} " \
                     "with consumer ARN: #{consumer_arn}")

        @shards[shard_id] = Kinesis::EnhancedShardReader.new(
          error_queue: @error_queue,
          logger: @logger,
          record_queue: @record_queue,
          shard_id: shard_id,
          sleep_time: @reader_sleep_time,
          consumer_arn: consumer_arn,
          starting_position: starting_position
        )
      end
    end
  end
end
