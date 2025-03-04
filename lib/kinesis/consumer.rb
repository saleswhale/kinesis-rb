# frozen_string_literal: true

require 'concurrent/hash'
require 'kinesis/shard_reader'
require 'kinesis/enhanced_shard_reader'
require 'kinesis/state'
require 'logger'

module Kinesis
  # Kinesis::Consumer
  class Consumer
    LOCK_DURATION = 30 # seconds
    READ_INTERVAL = 0.05 # seconds
    DEFAULT_PUSH_LIMIT = 1000

    def initialize(
      stream_name:,
      dynamodb: { client: nil, table_name: nil, consumer_group: nil },
      kinesis: { client: nil },
      lock_duration: LOCK_DURATION,
      logger: nil,
      reader_sleep_time: nil,
      push_limit: DEFAULT_PUSH_LIMIT,
      pull_limit: nil,
      use_enhanced_fan_out: false,
      consumer_name: nil
    )
      @dynamodb = dynamodb
      @error_queue = Queue.new
      @lock_duration = lock_duration
      @kinesis_client = kinesis[:client] || Aws::Kinesis::Client.new
      @reader_sleep_time = reader_sleep_time
      @record_queue = SizedQueue.new(push_limit)
      @shards = Concurrent::Hash.new
      @stream_name = stream_name
      @logger = logger || Logger.new($stdout)
      @pull_limit = pull_limit
      @state = nil
      @use_enhanced_fan_out = use_enhanced_fan_out
      @consumer_name = consumer_name
      
      if @use_enhanced_fan_out && @consumer_name.nil?
        raise ArgumentError, "consumer_name is required when using enhanced fan-out"
      end
    end

    def each(&block)
      trap('INT') { raise SignalException, 'SIGTERM' }

      @stream_info = @kinesis_client.describe_stream(stream_name: @stream_name)
      @state = State.new(
        dynamodb: @dynamodb,
        logger: @logger,
        stream_name: @stream_name,
        stream_retention_period_in_hours: @stream_info.dig(:stream_description, :retention_period_hours)
      )

      # Register consumer if using enhanced fan-out
      register_consumer if @use_enhanced_fan_out

      loop do
        setup_shards
        setup_time = Time.now

        loop do
          # @lock_duration - 1 because we want to refresh just before it expires
          break if (Time.now - setup_time) > (@lock_duration - 1)

          wait_for_records { |item| block.call(item) }

          sleep READ_INTERVAL # without sleep, CPU utilization shoots up 100%
        end
      end
    rescue SignalException => e
      raise e unless %w[SIGTERM SIGINT].include?(e.to_s)
    ensure
      shutdown
    end

    private

    def register_consumer
      return unless @use_enhanced_fan_out
      
      begin
        # Check if consumer already exists
        @kinesis_client.describe_consumer(
          consumer_arn: consumer_arn
        )
        @logger.info("Using existing consumer: #{@consumer_name}")
      rescue Aws::Kinesis::Errors::ResourceNotFoundException
        # Create consumer if it doesn't exist
        response = @kinesis_client.register_stream_consumer(
          stream_arn: stream_arn,
          consumer_name: @consumer_name
        )
        @logger.info("Registered new consumer: #{response.consumer.consumer_name}")
      end
    end

    def stream_arn
      @stream_info.dig(:stream_description, :stream_arn)
    end

    def consumer_arn
      "#{stream_arn}/consumer/#{@consumer_name}"
    end

    # 1 thread per shard, will indefinitely push to queue
    def setup_shards
      @shards.each do |shard_id, shard|
        shutdown_shard_reader(shard_id) unless shard.alive?
      end

      response = @kinesis_client.list_shards(
        stream_name: @stream_name,
        shard_filter: { type: 'AT_LATEST' }
      )

      response.shards.each do |shard_data|
        shard_id = shard_data[:shard_id]
        shard_locked = @state.lock_shard(shard_id, Time.now + LOCK_DURATION)

        if shard_locked
          start_shard_reader(shard_id) unless @shards.key?(shard_id)
        else
          shutdown_shard_reader(shard_id)
        end
      end
    end

    def wait_for_records
      return if @record_queue.empty?

      shard_id, item = @record_queue.pop

      @logger.info(
        {
          message: 'Got record',
          item_approximate_arrival_timestamp: item.approximate_arrival_timestamp,
          item_partition_key: item.partition_key,
          item_sequence_number: item.sequence_number
        }
      )

      yield item

      save_checkpoint(shard_id, item)
    end

    def save_checkpoint(shard_id, item)
      @state.checkpoint(shard_id, item[:sequence_number])
    rescue StandardError
      shutdown_shard_reader(shard_id)
    end

    def shutdown_shard_reader(shard_id)
      shard = @shards[shard_id]

      return unless shard

      shard.shutdown

      @shards.delete(shard_id)
    end

    def start_shard_reader(shard_id)
      shard_iterator = get_shard_iterator(shard_id)

      @shards[shard_id] = Kinesis::ShardReader.new(
        error_queue: @error_queue,
        logger: @logger,
        record_queue: @record_queue,
        shard_id: shard_id,
        shard_iterator: shard_iterator,
        sleep_time: @reader_sleep_time,
        pull_limit: @pull_limit
      )
    end

    def start_enhanced_shard_reader(shard_id)
      iterator_args = @state.get_iterator_args(shard_id)
      starting_position = {
        type: iterator_args[:shard_iterator_type]
      }
      
      # Add sequence number if needed
      if iterator_args[:shard_iterator_type] == 'AFTER_SEQUENCE_NUMBER' || 
         iterator_args[:shard_iterator_type] == 'AT_SEQUENCE_NUMBER'
        starting_position[:sequence_number] = iterator_args[:starting_sequence_number]
      elsif iterator_args[:shard_iterator_type] == 'AT_TIMESTAMP'
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

    def shutdown
      @shards.values.each(&:shutdown)
      @shards = Concurrent::Hash.new
    end

    def get_shard_iterator(shard_id)
      iterator_args = @state.get_iterator_args(shard_id)

      @kinesis_client.get_shard_iterator(
        stream_name: @stream_name,
        shard_id: shard_id,
        **iterator_args
      )[:shard_iterator]
    end
  end
end
