require "concurrent/executor/thread_pool_executor"
require "concurrent/executor/immediate_executor"
require "concurrent/configuration"

module Sentry
  class BackgroundWorker
    include LoggingHelper

    attr_reader :max_queue, :number_of_threads, :logger
    attr_accessor :shutdown_timeout

    def initialize(configuration)
      @max_queue = 30
      @shutdown_timeout = 1
      @number_of_threads = configuration.background_worker_threads
      @logger = configuration.logger
      @shutdown_callback = nil

      @executor =
        if configuration.async
          log_debug("config.async is set, BackgroundWorker is disabled")
          Concurrent::ImmediateExecutor.new
        elsif @number_of_threads == 0
          log_debug("config.background_worker_threads is set to 0, all events will be sent synchronously")
          Concurrent::ImmediateExecutor.new
        else
          log_debug("initialized a background worker with #{@number_of_threads} threads")

          executor = Concurrent::ThreadPoolExecutor.new(
            min_threads: 0,
            max_threads: @number_of_threads,
            max_queue: @max_queue,
            fallback_policy: :discard
          )

          @shutdown_callback = proc do
            executor.shutdown
            executor.wait_for_termination(@shutdown_timeout)
          end

          executor
        end
    end

    def perform(&block)
      @executor.post do
        block.call
      end
    end

    def shutdown
      @shutdown_callback&.call
    end
  end
end
