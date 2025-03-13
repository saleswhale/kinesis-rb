# frozen_string_literal: true

require 'socket'
require 'time'
require 'kinesis/state/lock_manager'

module Kinesis
  # Kinesis::State
  class State
    include Kinesis::State::LockManager

    attr_accessor :shards

    # dynamodb[:consumer_group] - Preferrably the name of the application using the gem,
    #                             otherwise will just default to the root dir
    def initialize(stream_name:, stream_retention_period_in_hours:, logger:, dynamodb: {})
      @consumer_group = dynamodb[:consumer_group] || File.basename(Dir.getwd)
      @consumer_id = Addrinfo.getaddrinfo(Socket.gethostname, nil, :INET).first.ip_address
      @dynamodb_client = dynamodb[:client]
      @dynamodb_table_name = dynamodb[:table_name]
      @logger = logger
      @shards = {}
      @stream_name = stream_name
      @stream_retention_period_in_hours = stream_retention_period_in_hours
      @key = {
        'consumerGroup': @consumer_group,
        'streamName': @stream_name
      }
    end

    def get_iterator_args(shard_id)
      iterator_args = { shard_iterator_type: 'LATEST' }

      return iterator_args unless @shards.key?(shard_id)

      heartbeat = @shards[shard_id]['heartbeat']
      last_sequence_number = @shards[shard_id]['checkpoint']

      return iterator_args unless heartbeat && last_sequence_number

      heartbeat_diff = Time.now.utc - Time.parse(heartbeat).utc

      if heartbeat_diff > (60 * 60 * @stream_retention_period_in_hours)
        @logger.info(
          {
            message: 'Heartbeat is stale, defaulting to LATEST',
            sequence_number: last_sequence_number,
            heartbeat: heartbeat
          }
        )
      else
        iterator_args = {
          shard_iterator_type: 'AFTER_SEQUENCE_NUMBER',
          starting_sequence_number: last_sequence_number
        }
      end

      iterator_args
    end

    def checkpoint(shard_id, sequence_number, options = {})
      return unless plugged_in?

      # Extract options
      is_efo = options[:efo] || false

      # Build the condition expression based on consumer type
      condition_expression = if is_efo
                               # Relaxed condition for EFO consumers
                               'consumerGroup = :consumer_group AND streamName = :stream_name'
                             else
                               # Strict condition for standard consumers - sequence must be greater
                               'consumerGroup = :consumer_group AND ' \
                               'streamName = :stream_name AND ' \
                               '(' \
                               '  attribute_not_exists(shards.#shard_id.checkpoint) OR ' \
                               '  shards.#shard_id.checkpoint < :sequence_number ' \
                               ')'
                             end

      begin
        @dynamodb_client.update_item(
          table_name: @dynamodb_table_name,
          key: @key,
          expression_attribute_names: {
            '#shard_id': shard_id
          },
          expression_attribute_values: {
            ':consumer_group': @consumer_group,
            ':sequence_number': sequence_number,
            ':heartbeat': Time.now.utc.iso8601,
            ':stream_name': @stream_name
          },
          condition_expression: condition_expression,
          update_expression:
            'SET ' \
            'shards.#shard_id.checkpoint = :sequence_number, ' \
            'shards.#shard_id.heartbeat = :heartbeat'
        )

        # Update local cache
        @shards[shard_id] ||= {}
        @shards[shard_id]['checkpoint'] = sequence_number
      rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException => e
        # Re-raise the exception with context
        raise e if is_efo

        # This shouldn't happen with relaxed conditions

        # For standard consumers, this is expected when sequence isn't greater
        raise StandardError, "Failed to checkpoint shard #{shard_id}: newer checkpoint exists"
      end
    end

    def dig_checkpoint(response, shard_id)
      return nil unless response&.item && response.item['shards']
      return nil unless response.item['shards'][shard_id]

      response.item['shards'][shard_id]['checkpoint']
    end

    private

    def plugged_in?
      !@dynamodb_client.nil? && !@dynamodb_table_name.nil?
    end
  end
end
