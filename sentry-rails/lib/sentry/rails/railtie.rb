require "sentry/rails/capture_exceptions"
require "sentry/rails/rescued_exception_interceptor"
require "sentry/rails/backtrace_cleaner"

module Sentry
  class Railtie < ::Rails::Railtie
    # middlewares can't be injected after initialize
    initializer "sentry.use_rack_middleware" do |app|
      # placed after all the file-sending middlewares so we can avoid unnecessary transactions
      app.config.middleware.insert_after ActionDispatch::ShowExceptions, Sentry::Rails::CaptureExceptions
      # need to place as close to DebugExceptions as possible to intercept most of the exceptions, including those raised by middlewares
      app.config.middleware.insert_after ActionDispatch::DebugExceptions, Sentry::Rails::RescuedExceptionInterceptor
    end

    # because the extension works by registering the around_perform callback, it should always be ran
    # before the application is eager-loaded (before user's jobs register their own callbacks)
    # See https://github.com/getsentry/sentry-ruby/issues/1249#issuecomment-853871871 for the detail explanation
    initializer "sentry.extend_active_job", before: :eager_load! do |app|
      ActiveSupport.on_load(:active_job) do
        require "sentry/rails/active_job"
        prepend Sentry::Rails::ActiveJobExtensions
      end
    end

    config.after_initialize do |app|
      next unless Sentry.initialized?

      configure_project_root
      configure_trusted_proxies
      extend_controller_methods if defined?(ActionController)
      patch_background_worker if defined?(ActiveRecord)
      override_streaming_reporter if defined?(ActionView)
      setup_backtrace_cleanup_callback
      inject_breadcrumbs_logger
      activate_tracing
    end

    runner do
      next unless Sentry.initialized?
      Sentry.configuration.background_worker_threads = 0
    end

    def configure_project_root
      Sentry.configuration.project_root = ::Rails.root.to_s
    end

    def configure_trusted_proxies
      Sentry.configuration.trusted_proxies += Array(::Rails.application.config.action_dispatch.trusted_proxies)
    end

    def extend_controller_methods
      require "sentry/rails/controller_methods"
      require "sentry/rails/controller_transaction"
      require "sentry/rails/overrides/streaming_reporter"

      ActiveSupport.on_load :action_controller do
        include Sentry::Rails::ControllerMethods
        include Sentry::Rails::ControllerTransaction
        ActionController::Live.send(:prepend, Sentry::Rails::Overrides::StreamingReporter)
      end
    end

    def patch_background_worker
      require "sentry/rails/background_worker"
    end

    def inject_breadcrumbs_logger
      if Sentry.configuration.breadcrumbs_logger.include?(:active_support_logger)
        require 'sentry/rails/breadcrumb/active_support_logger'
        Sentry::Rails::Breadcrumb::ActiveSupportLogger.inject
      end

      if Sentry.configuration.breadcrumbs_logger.include?(:monotonic_active_support_logger)
        return warn "Usage of `monotonic_active_support_logger` require a version of Rails >= 6.1, please upgrade your Rails version or use another logger" if ::Rails.version.to_f < 6.1

        require 'sentry/rails/breadcrumb/monotonic_active_support_logger'
        Sentry::Rails::Breadcrumb::MonotonicActiveSupportLogger.inject
      end
    end

    def setup_backtrace_cleanup_callback
      backtrace_cleaner = Sentry::Rails::BacktraceCleaner.new

      Sentry.configuration.backtrace_cleanup_callback ||= lambda do |backtrace|
        backtrace_cleaner.clean(backtrace)
      end
    end

    def override_streaming_reporter
      require "sentry/rails/overrides/streaming_reporter"

      ActiveSupport.on_load :action_view do
        ActionView::StreamingTemplateRenderer::Body.send(:prepend, Sentry::Rails::Overrides::StreamingReporter)
      end
    end

    def activate_tracing
      if Sentry.configuration.tracing_enabled?
        subscribers = Sentry.configuration.rails.tracing_subscribers
        Sentry::Rails::Tracing.register_subscribers(subscribers)
        Sentry::Rails::Tracing.subscribe_tracing_events
        Sentry::Rails::Tracing.patch_active_support_notifications
      end
    end
  end
end
