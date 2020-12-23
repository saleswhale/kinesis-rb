# frozen_string_literal: true

module Kinesis
  # Kinesis::Subthread
  class Subthread
    attr_reader :thread
    delegate :alive?, to: :thread

    def initialize(_)
      @thread = nil
      start
    end

    def start
      @thread = Thread.new { run }
      @thread.report_on_exception = true
    end

    def run
      raise NotImplementedError
    end
  end

  # Kinesis::SubthreadLoop
  class SubthreadLoop < Subthread
    # http://docs.aws.amazon.com/streams/latest/dev/kinesis-low-latency.html
    def run
      preprocess

      loop do
        sleep_time = process

        if sleep_time
          sleep sleep_time
        else # sleep_time is false or nil
          break
        end
      end
    end

    def preprocess
      raise NotImplementedError
    end

    def process
      raise NotImplementedError
    end
  end
end
