# frozen_string_literal: true

module Kinesis
  # Provides error handling functionality for EnhancedShardReader
  # Handles various error types that may occur during Enhanced Fan-Out operations
  # including JSON parsing errors, AWS service errors, and HTTP/2 stream errors.
  module EnhancedShardReaderErrorHandling
    def handle_error(error)
      error_message = format_error_message(error)

      if error.is_a?(Seahorse::Client::Http2StreamInitializeError)
        log_http2_error(error_message)
      else
        log_detailed_error(error, error_message)
      end

      @error_queue << error
    end

    def format_error_message(error)
      case error
      when JSON::ParserError
        "JSON parsing error in enhanced fan-out for shard #{@shard_id}: #{error.message}"
      when Aws::Errors::ServiceError
        "AWS service error in enhanced fan-out for shard #{@shard_id}: " \
        "#{error.message} (#{error.class})"
      when Seahorse::Client::Http2StreamInitializeError
        "HTTP/2 stream initialization error for shard #{@shard_id}: " \
        "#{error.message} - #{error.original_error.message}"
      else
        "Error setting up enhanced fan-out for shard #{@shard_id}: " \
        "#{error.message} (#{error.class})"
      end
    end

    def log_http2_error(error_message)
      @logger.warn(error_message)
      @logger.warn('Will retry the subscription')
    end

    def log_detailed_error(error, error_message)
      @logger.error(error_message)

      case error
      when JSON::ParserError
        log_json_error_details
      when Aws::Errors::ServiceError
        log_aws_error_details(error)
      else
        @logger.error(error.backtrace.join("\n")) if error.backtrace
      end
    end

    def log_json_error_details
      @logger.error("Consumer ARN: #{@consumer_arn}")
      @logger.error("Starting position: #{@starting_position.inspect}")
    end

    def log_aws_error_details(error)
      # Log additional details if available
      error_details = []
      error_details << "Code: #{error.code}" if error.respond_to?(:code)
      if error.respond_to?(:context) && error.context.respond_to?(:request_id)
        error_details << "Request ID: #{error.context.request_id}"
      end
      @logger.error("Error details: #{error_details.join(', ')}") unless error_details.empty?
    end
  end
end
