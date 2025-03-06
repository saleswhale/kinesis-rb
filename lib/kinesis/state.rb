# frozen_string_literal: true

require 'socket'
require 'time'

module Kinesis
  # Kinesis::State
  # rubocop:disable Metrics/ClassLength
  class State
    attr_accessor :shards

    # dynamodb[:consumer_group] - Preferrably the name of the application using the gem,
    #                             otherwise will just default to the root dir
    def initialize(stream_name:, stream_retention_period_in_hours:, logger:, dynamodb: {})
      @consumer_group = dynamodb[:consumer_group] || File.basename(Dir.getwd)
      @dynamodb_client = dynamodb[:client]
      @dynamodb_table_name = dynamodb[:table_name]
      @logger = logger
      @consumer_id = generate_consumer_id
      @shards = {}
      @stream_name = stream_name
      @stream_retention_period_in_hours = stream_retention_period_in_hours
      @key = {
        'consumerGroup': @consumer_group,
        'streamName': @stream_name
      }
    end

    def generate_consumer_id
      # Try the standard hostname resolution first
      Addrinfo.getaddrinfo(Socket.gethostname, nil, :INET).first.ip_address
    rescue StandardError => e
      # Fall back to alternatives if hostname resolution fails
      @logger.warn("Hostname resolution failed: #{e.message}. Using fallback consumer ID.")

      # Option 1: Use environment variable if set
      return ENV['KINESIS_CONSUMER_ID'] if ENV['KINESIS_CONSUMER_ID']

      # Option 2: Generate a unique ID combining process ID and timestamp
      "consumer-#{Process.pid}-#{Time.now.to_i}"
    end

    def get_iterator_args(shard_id)
      iterator_args = { shard_iterator_type: 'LATEST' }

      return iterator_args unless @shards.key?(shard_id)

      heartbeat = @shards[shard_id]['heartbeat']
      last_sequence_number = @shards[shard_id]['checkpoint']

      return iterator_args unless heartbeat && last_sequence_number

      heartbeat_diff = Time.now.utc - Time.parse(heartbeat).utc

      if heartbeat_diff > (60 * 60 * @stream_retention_period_in_hours)
        log_stale_heartbeat(last_sequence_number, heartbeat)
      else
        iterator_args = {
          shard_iterator_type: 'AFTER_SEQUENCE_NUMBER',
          starting_sequence_number: last_sequence_number
        }
      end

      iterator_args
    end

    def checkpoint(shard_id, sequence_number)
      return unless plugged_in?

      @dynamodb_client.update_item(
        table_name: @dynamodb_table_name,
        key: @key,
        expression_attribute_names: {
          '#shard_id': shard_id
        },
        expression_attribute_values: checkpoint_expression_values(sequence_number),
        condition_expression: checkpoint_condition_expression,
        update_expression: checkpoint_update_expression
      )

      @shards[shard_id]['checkpoint'] = sequence_number
    end

    def lock_shard(shard_id, expire_time)
      return true unless plugged_in?

      resp = @dynamodb_client.get_item(
        table_name: @dynamodb_table_name,
        consistent_read: true,
        key: @key
      )

      if resp[:item]
        shard = resp[:item].dig('shards', shard_id)

        return false if shard_locked_by_other_consumer?(shard, shard_id)

        @shards[shard_id] = shard
      end

      if @shards[shard_id].nil?
        create_new_lock(expire_time, shard_id)
      else
        update_lock(expire_time, shard_id)
      end

      true
    rescue StandardError => e
      raise e unless Kinesis::RETRYABLE_EXCEPTIONS.include?(e.class.name)

      @logger.info(
        {
          message: 'Throttled while trying to read lock table in Dynamo'
        }
      )
      sleep 1
      retry
    end

    private

    def log_stale_heartbeat(last_sequence_number, heartbeat)
      @logger.info(
        {
          message: 'Heartbeat is stale, defaulting to LATEST',
          sequence_number: last_sequence_number,
          heartbeat: heartbeat
        }
      )
    end

    def checkpoint_expression_values(sequence_number)
      {
        ':consumer_group': @consumer_group,
        ':sequence_number': sequence_number,
        ':heartbeat': Time.now.utc.iso8601,
        ':stream_name': @stream_name
      }
    end

    def checkpoint_condition_expression
      'consumerGroup = :consumer_group AND ' \
      'streamName = :stream_name AND ' \
      '(' \
      '  attribute_not_exists(shards.#shard_id.checkpoint) OR ' \
      '  shards.#shard_id.checkpoint < :sequence_number ' \
      ')'
    end

    def checkpoint_update_expression
      'SET ' \
      'shards.#shard_id.checkpoint = :sequence_number, ' \
      'shards.#shard_id.heartbeat = :heartbeat'
    end

    def shard_locked_by_other_consumer?(shard, shard_id)
      if shard && shard['consumerId'] != @consumer_id && Time.parse(shard['expiresIn']) > Time.now
        @logger.info(
          {
            message: 'Not starting reader for shard as it is locked by a different consumer',
            consumer_id: @consumer_id,
            locked_consumer_id: shard['consumerId'],
            locked_expiry: shard['expiresIn'],
            shard_id: shard_id
          }
        )
        return true
      end
      false
    end

    def create_new_lock(expire_time, shard_id, retry_once_on_failure: true)
      shard = {
        'consumerId' => @consumer_id,
        'expiresIn' => expire_time.utc.iso8601,
        'heartbeat' => Time.now.utc.iso8601
      }

      @dynamodb_client.update_item(
        table_name: @dynamodb_table_name,
        key: @key,
        expression_attribute_names: {
          '#shard_id': shard_id
        },
        expression_attribute_values: {
          ':shard': shard
        },
        condition_expression: 'attribute_not_exists(shards.#shard_id)',
        update_expression: 'SET shards.#shard_id = :shard'
      )

      @shards[shard_id] = shard
    rescue Aws::DynamoDB::Errors::ValidationException => e
      raise e unless retry_once_on_failure

      # possible that `shards -> consumer_group` item doesn't exist yet
      @dynamodb_client.update_item(
        table_name: @dynamodb_table_name,
        key: @key,
        expression_attribute_values: { ':empty' => {} },
        update_expression: 'SET shards = if_not_exists(shards, :empty)'
      )
      retry_once_on_failure = false
      retry
    end

    def update_lock(expire_time, shard_id)
      current_consumer_id = @shards[shard_id]['consumerId']
      current_expiry = Time.parse(@shards[shard_id]['expiresIn']).iso8601

      new_expiry = expire_time.utc.iso8601
      now = Time.now.utc.iso8601

      @dynamodb_client.update_item(
        table_name: @dynamodb_table_name,
        key: @key,
        expression_attribute_names: {
          '#shard_id': shard_id
        },
        expression_attribute_values: update_lock_expression_values(current_consumer_id, current_expiry, new_expiry,
                                                                   now),
        condition_expression: update_lock_condition_expression,
        update_expression: update_lock_update_expression
      )

      # NOTE: Only update affected keys, don't replace the whole object
      @shards[shard_id].merge!(
        {
          'consumerId' => @consumer_id,
          'expiresIn' => new_expiry,
          'heartbeat' => now
        }
      )
    end

    def update_lock_expression_values(current_consumer_id, current_expiry, new_expiry, now)
      {
        ':new_consumer_id': @consumer_id,
        ':new_expires': new_expiry,
        ':current_consumer_id': current_consumer_id,
        ':current_expires': current_expiry,
        ':heartbeat': now
      }
    end

    def update_lock_condition_expression
      'shards.#shard_id.consumerId = :current_consumer_id AND ' \
      'shards.#shard_id.expiresIn = :current_expires'
    end

    def update_lock_update_expression
      'SET ' \
      'shards.#shard_id.consumerId = :new_consumer_id, ' \
      'shards.#shard_id.expiresIn = :new_expires, ' \
      'shards.#shard_id.heartbeat = :heartbeat'
    end

    def plugged_in?
      !@dynamodb_client.nil? && !@dynamodb_table_name.nil?
    end
  end
  # rubocop:enable Metrics/ClassLength
end
