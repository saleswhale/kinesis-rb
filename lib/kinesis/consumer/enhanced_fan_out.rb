# frozen_string_literal: true

module Kinesis
  class Consumer
    # Module for Enhanced Fan-Out functionality
    module EnhancedFanOut
      def register_consumer
        return unless @use_enhanced_fan_out

        begin
          # Check if consumer already exists using stream_arn and consumer_name
          response = @kinesis_client.describe_stream_consumer(
            stream_arn: stream_arn,
            consumer_name: @consumer_name
          )
          @consumer_arn = response.consumer_description.consumer_arn
          @logger.info("Using existing consumer: #{@consumer_name}, ARN: #{@consumer_arn}")
        rescue Aws::Kinesis::Errors::ResourceNotFoundException
          # Create consumer if it doesn't exist
          response = @kinesis_client.register_stream_consumer(
            stream_arn: stream_arn,
            consumer_name: @consumer_name
          )
          @consumer_arn = response.consumer.consumer_arn
          @logger.info("Registered new consumer: #{response.consumer.consumer_name}, ARN: #{@consumer_arn}")
        end
      end

      def consumer_arn
        @consumer_arn || raise('Consumer ARN not available. Make sure register_consumer is called first.')
      end

      def start_enhanced_shard_reader(shard_id)
        iterator_args = @state.get_iterator_args(shard_id)
        starting_position = {
          type: iterator_args[:shard_iterator_type]
        }

        # Add sequence number if needed
        case iterator_args[:shard_iterator_type]
        when 'AFTER_SEQUENCE_NUMBER', 'AT_SEQUENCE_NUMBER'
          starting_position[:sequence_number] = iterator_args[:starting_sequence_number]
        when 'AT_TIMESTAMP'
          starting_position[:timestamp] = iterator_args[:timestamp]
        end

        @shards[shard_id] = Kinesis::EnhancedShardReader.new(
          error_queue: @error_queue,
          logger: @logger,
          record_queue: @record_queue,
          shard_id: shard_id,
          sleep_time: @reader_sleep_time,
          kinesis_client: @kinesis_client,
          consumer_arn: consumer_arn,
          starting_position: starting_position
        )
      end
    end
  end
end
