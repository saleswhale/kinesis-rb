# frozen_string_literal: true

require 'concurrent/hash'
require 'kinesis/shard_reader'
require 'kinesis/state'

module Kinesis
  # Kinesis::Consumer
  class Consumer
    LOCK_DURATION = 30 # seconds

    def initialize(
      stream_name:,
      reader_sleep_time: nil,
      lock_duration: LOCK_DURATION,
      kinesis: { client: nil },
      dynamodb: { client: nil, table_name: nil, consumer_group: nil }
    )
      @error_queue = Queue.new
      @lock_duration = lock_duration
      @kinesis_client = kinesis[:client] || Aws::Kinesis::Client.new
      @reader_sleep_time = reader_sleep_time
      @record_queue = Queue.new
      @shards = Concurrent::Hash.new
      @state = State.new(dynamodb: dynamodb, stream_name: stream_name)
      @stream_data = nil
      @stream_name = stream_name
    end

    def each
      trap('INT') { raise SignalException.new('SIGTERM') }

      loop do
        setup_shards
        setup_time = Time.now

        loop do
          # @lock_duration - 1 because we want to refresh just before it expires
          break if (Time.now - setup_time) > (@lock_duration - 1)

          wait_for_records { |item| yield item }
        end
      end
    rescue SignalException => e
      raise e unless ['SIGTERM', 'SIGINT'].include?(e.to_s)
    ensure
      shutdown
    end

    private

    # 1 thread per shard, will indefinitely push to queue
    def setup_shards
      @stream_data = @kinesis_client.describe_stream(stream_name: @stream_name)

      @stream_data.dig(:stream_description, :shards).each do |shard_data|
        shard_id = shard_data[:shard_id]

        lock_shard(shard_id)

        create_shard_reader(shard_id) unless @shards.key?(shard_id)
      end
    end

    # lock when able
    def lock_shard(shard_id)
      shard_locked = @state.lock_shard(shard_id, Time.now + LOCK_DURATION)

      return if shard_locked

      # we somehow lost the lock, shutdown the reader
      shutdown_shard_reader(shard_id) if @shards.keys.include?(shard_id)
    end

    def wait_for_records
      return if @record_queue.empty?

      shard_id, resp = @record_queue.pop

      resp[:records].each do |item|
        # Log: Got record
        yield item

        save_checkpoint(shard_id, item)
      end
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

    def create_shard_reader(shard_id)
      shard_iterator = get_shard_iterator(shard_id)

      @shards[shard_id] = Kinesis::ShardReader.new(
        error_queue: @error_queue,
        record_queue: @record_queue,
        shard_id: shard_id,
        shard_iterator: shard_iterator,
        sleep_time: @reader_sleep_time
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
      ).dig(:shard_iterator)
    end
  end
end
