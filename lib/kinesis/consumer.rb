# frozen_string_literal: true

require 'concurrent/hash'
require 'kinesis/shard_reader'
require 'kinesis/state'

module Kinesis
  # Kinesis::Consumer
  class Consumer
    LOCK_DURATION = 30

    def initialize(stream_name:, state: nil, reader_sleep_time: nil)
      @error_queue = Queue.new
      @kinesis_client = Aws::Kinesis::Client.new
      @reader_sleep_time = reader_sleep_time
      @record_queue = Queue.new
      @run = true
      @shards = Concurrent::Hash.new
      @state = state
      @stream_data = nil
      @stream_name = stream_name
    end

    def each
      setup_shards
      wait_for_records while @run
    rescue Interrupt
      @run = false
    rescue SignalException
      @run = false
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

      @shards.values.map(&:thread).each(&:join)
    end

    # lock when able
    def lock_shard(shard_id)
      return unless @state

      shard_locked = @state.lock_shard(
        state_shard_id(shard_data[:shard_id]),
        LOCK_DURATION
      )

      return if shard_locked

      # we somehow lost the lock, shutdown the reader
      shutdown_shard_reader(shard_id) if @shards.keys.include?(shard_id)
    end

    def wait_for_records
      return if @record_queue.empty?

      shard_id, resp = @record.pop

      resp['Records'].each do |item|
        # Log: Got record
        puts "---- got item: #{item}"

        yield item

        save_checkpoint(shard_id, item)
      end
    end

    def save_checkpoint(shard_id, item)
      return unless @state

      @state.checkpoint(
        state_shard_id(shard_id),
        item['SequenceNumber']
      )
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
      @shards = {}
      @run = false
    end

    def state_shard_id(shard_id)
      [@stream_name, shard_id].join('_')
    end

    def get_shard_iterator(shard_id)
      iterator_args = if @state
                        @state.get_iterator_args(shard_id)
                      else
                        { shard_iterator_type: 'LATEST' }
                      end

      @kinesis_client.get_shard_iterator(
        stream_name: @stream_name,
        shard_id: shard_id,
        **iterator_args
      ).dig(:shard_iterator)
    end
  end
end
