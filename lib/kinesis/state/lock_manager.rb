# frozen_string_literal: true

module Kinesis
  class State
    # Manages shard locks in DynamoDB for Kinesis consumers
    # rubocop:disable Metrics/ModuleLength
    module LockManager
      def lock_shard(shard_id, expires_in)
        return true unless plugged_in?

        resp = @dynamodb_client.get_item(
          table_name: @dynamodb_table_name,
          consistent_read: true,
          key: @key
        )

        if resp[:item]
          shard = resp[:item].dig('shards', shard_id)

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
            return false
          end

          @shards[shard_id] = shard
        end

        if @shards[shard_id].nil?
          create_new_lock(expires_in, shard_id)
        else
          update_lock(expires_in, shard_id)
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

      def create_new_lock(expires_in, shard_id, retry_once_on_failure: true)
        shard = {
          'consumerId' => @consumer_id,
          'expiresIn' => expires_in.utc.iso8601,
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

      def update_lock(expires_in, shard_id)
        current_consumer_id = @shards[shard_id]['consumerId']
        current_expiry = Time.parse(@shards[shard_id]['expiresIn']).iso8601

        new_expiry = expires_in.utc.iso8601
        now = Time.now.utc.iso8601

        @dynamodb_client.update_item(
          table_name: @dynamodb_table_name,
          key: @key,
          expression_attribute_names: {
            '#shard_id': shard_id
          },
          expression_attribute_values: {
            ':new_consumer_id': @consumer_id,
            ':new_expires': new_expiry,
            ':current_consumer_id': current_consumer_id,
            ':current_expires': current_expiry,
            ':heartbeat': now
          },
          condition_expression:
            'shards.#shard_id.consumerId = :current_consumer_id AND ' \
            'shards.#shard_id.expiresIn = :current_expires',

          # NOTE: Only update affected keys, don't replace the whole object, so as
          # to not override rolling updates during `checkpoint` call
          update_expression:
            'SET ' \
            'shards.#shard_id.consumerId = :new_consumer_id, ' \
            'shards.#shard_id.expiresIn = :new_expires, ' \
            'shards.#shard_id.heartbeat = :heartbeat'
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
    end
    # rubocop:enable Metrics/ModuleLength
  end
end
