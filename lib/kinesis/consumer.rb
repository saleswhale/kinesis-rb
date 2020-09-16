# frozen_string_literal: true

module Kinesis
  # Kinesis::Consumer
  class Consumer
    def initialize(stream_name:, kinesis_client: nil, state: nil)
      @error_queue = Queue.new
      @kinesis_client = kinesis_client || Aws::Kinesis::Client.new
      @record_queue = Queue.new
      @run = true
      @state = state
      @stream_name = stream_name
    end

    def each
      while @run
        setup_shards
        read_record
      end
    rescue Interrupt
      @run = false
    rescue SignalException
      @run = false
    ensure
      shutdown
    end

    private

    def setup_shards
      # TODO
    end

    def read_record
      return if @record_queue.empty?

      shard_id, resp = @record.pop

      resp['Records'].each do |item|
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

    def shutdown
      @shards.each(&:shutdown)
      @shards = {}
      @run = false
    end
  end
end
