# frozen_string_literal: true

module Kinesis
  # Kinesis::Subthread
  class Subthread
    def initialize(**kwargs)
      @wait_for_child = kwargs[:wait_for_child] || false
      @thread = nil

      start
    end

    def start
      @thread = Thread.new { run }
    end

    def run
      raise NotImplementedError
    end
  end

  # Kinesis::SubthreadLoop
  class SubthreadLoop < Subthread
    def initialize(**kwargs)
      super(**kwargs, wait_for_child: false)
    end

    # http://docs.aws.amazon.com/streams/latest/dev/kinesis-low-latency.html
    def run
      alive = true

      preprocess

      while alive
        sleep_time = process

        if sleep_time
          sleep sleep_time
        else # sleep_time is false or nil
          alive = false
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
