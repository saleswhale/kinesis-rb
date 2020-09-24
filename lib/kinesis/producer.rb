# frozen_string_literal: true

require 'kinesis/subthread_loop'
require 'objspace'

module Kinesis
  # Kinesis::AsyncProducer
  class AsyncProducer < SubthreadLoop
    # * The maximum size of a data blob (the data payload before base64-encoding) is up to 1 MB.
    # * Each shard can support up to 1,000 records per second for writes, up to a maximum total data write rate of 1 MB
    #   per second (including partition keys).
    # * PutRecords supports up to 500 records in a single call
    MAX_RECORDS_SIZE = (2**20)
    MAX_RECORDS_COUNT = 500

    def initialize(stream_name:, buffer_time:, record_queue:)
      @buffer_time = buffer_time
      @main_record_queue = record_queue
      @stream_name = stream_name

      super
    end

    def preprocess
      @kinesis_client = Aws::Kinesis::Client.new
      @next_record_queue = Queue.new
      @record_count = 0
      @record_queue = Queue.new
      @record_size = 0
      @retries = 0
      @timer_start = Time.now
    end

    def process
      while (Time.now - @timer_start) < @buffer_time
        next if @main_record_queue.empty?

        # record, explicit_hash_key, partition_key = @main_record_queue.pop
        record = @main_record_queue.pop
        partition_key = Time.now.to_f.to_s

        record = {
          data: record.to_json,
          partition_key: partition_key
        }

        @record_size += ObjectSpace.memsize_of(record)

        if @record_size >= MAX_RECORDS_SIZE
          # Log: Records exceed MAX_RECORDS_SIZE (#{MAX_RECORDS_SIZE})! Adding to next_records: #{record}
          @next_record_queue << record
          break
        end

        @record_queue << record
        @record_count += 1

        if @record_count >= MAX_RECORDS_COUNT
          # Log: Records have reached MAX_RECORDS_COUNT (#{MAX_RECORDS_COUNT})! Flushing records
          break
        end
      end

      flush_records
      0 # sleep_time
    end

    def flush_records
      @timer_start = Time.now

      return if @record_queue.empty?

      # Log: Flushing #{@record_queue.size} records
      records = []

      @record_queue.size.times { records << @record_queue.pop }

      @kinesis_client.put_records(
        records: records,
        stream_name: @stream_name
      )

      @next_record_queue.size.times { @record_queue << @next_record_queue.pop }
      @record_count = 0
      @record_size = 0
    end
  end

  # Kinesis::Producer
  class Producer
    def initialize(stream_name:, buffer_time: 0.5)
      @record_queue = Queue.new
      @async_producer = AsyncProducer.new(
        record_queue: @record_queue,
        stream_name: stream_name,
        buffer_time: buffer_time
      )
    end

    def put(data)
      @record_queue << data
    end
  end
end
