# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/json"
require "uri"
require "logstash/plugin_mixins/http_client"
require "zlib"

class LogStash::Outputs::Http < LogStash::Outputs::Base
  include LogStash::PluginMixins::HttpClient
  
  concurrency :shared

  attr_accessor :is_batch
  attr_accessor :event_count

  VALID_METHODS = ["put", "post", "patch", "delete", "get", "head"]
  
  RETRYABLE_MANTICORE_EXCEPTIONS = [
    ::Manticore::Timeout,
    ::Manticore::SocketException,
    ::Manticore::ClientProtocolException, 
    ::Manticore::ResolutionFailure, 
    ::Manticore::SocketTimeout
  ]

  # This output lets you send events to a
  # generic HTTP(S) endpoint
  #
  # This output will execute up to 'pool_max' requests in parallel for performance.
  # Consider this when tuning this plugin for performance.
  #
  # Additionally, note that when parallel execution is used strict ordering of events is not
  # guaranteed!
  #
  # This gem now supports codecs. The format parameter is still around for backwards compatibility. 

  config_name "http"

  default :codec, "json"

  # URL to use
  config :url, :validate => :string, :required => :true

  # The HTTP Verb. One of "put", "post", "patch", "delete", "get", "head"
  config :http_method, :validate => VALID_METHODS, :required => :true

  # Custom headers to use
  # format is `headers => ["X-My-Header", "%{host}"]`
  config :headers, :validate => :hash, :default => {}

  # Content type
  #
  # If not specified, this defaults to the following:
  #
  # * if format is "json", "application/json"
  # * if format is "form", "application/x-www-form-urlencoded"
  config :content_type, :validate => :string
  
  # Set this to false if you don't want this output to retry failed requests
  config :retry_failed, :validate => :boolean, :default => true
  
  # If encountered as response codes this plugin will retry these requests
  config :retryable_codes, :validate => :number, :list => true, :default => [429, 500, 502, 503, 504]
  
  # If you would like to consider some non-2xx codes to be successes 
  # enumerate them here. Responses returning these codes will be considered successes
  config :ignorable_codes, :validate => :number, :list => true

  # This lets you choose the structure and parts of the event that are sent.
  # This only works with the json codec or the json and json_batch formats.
  #
  # Similar functionality should be added to the uri codec plugin in order to make this work for URI encoded data again.
  #
  # For example:
  # [source,ruby]
  #    mapping => {"foo" => "%{host}"
  #               "bar" => "%{type}"}
  config :mapping, :validate => :hash

  # Set the format of the http body.
  #
  # Now that we have codec support it is recommended that you use an appropriate codec instead of this parameter.
  # The one possible exception is the json_batch format.
  #
  # All of the functionality of the form format (converting data to a URI encoded string) has been moved into the uri codec.
  #
  # Instead of using the message format, use the plain codec and its format parameter.
  #
  # Defaults to json (which is also the default for the codec) 
  config :format, :validate => ["json", "json_batch", "form", "message"], :default => "json"

  # Set this to true if you want to enable gzip compression for your http requests
  config :http_compression, :validate => :boolean, :default => false
  
  # This doesn't do anything anymore. Use the format parameter of the plain codec instead.
  config :message, :validate => :string

  def register
    @http_method = @http_method.to_sym

    # We count outstanding requests with this queue
    # This queue tracks the requests to create backpressure
    # When this queue is empty no new requests may be sent,
    # tokens must be added back by the client on success
    @request_tokens = SizedQueue.new(@pool_max)
    @pool_max.times {|t| @request_tokens << true }

    @requests = Array.new

    if @content_type.nil?
      case @format
        when "form" ; @content_type = "application/x-www-form-urlencoded"
        when "json" ; @content_type = "application/json"
        when "json_batch" ; @content_type = "application/json"
        when "message" ; @content_type = "text/plain"
      end
    end

    @is_batch = @format == "json_batch"

    @headers["Content-Type"] = @content_type

    validate_format!
    
    # Run named Timer as daemon thread
    @timer = java.util.Timer.new("HTTP Output #{self.params['id']}", true)

    @codec.on_event do |event, data|
      process_event_data(event, data)
    end
  end # def register

  def process_event_data(event, data)
    # Override data when using the mapping parameter
    # Hack around the json codec not having equivalent functionality
    if @mapping 
      if @format == "json_batch"
        data = LogStash::Json.dump(event.map {|e| map_event(e) })
      elsif @format == "json" or @codec == "json"
        data = LogStash::Json.dump(map_event(event))
      end
    end

    successes = java.util.concurrent.atomic.AtomicInteger.new(0)
    failures  = java.util.concurrent.atomic.AtomicInteger.new(0)
    retries = java.util.concurrent.atomic.AtomicInteger.new(0)

    pending = Queue.new
    pending << [event, 0]

    while popped = pending.pop
      break if popped == :done

      event, attempt = popped

      send_event(event, data, attempt) do |action,event,attempt|
        begin 
          action = :failure if action == :retry && !@retry_failed
          
          case action
          when :success
            successes.incrementAndGet
          when :retry
            retries.incrementAndGet
            
            next_attempt = attempt+1
            sleep_for = sleep_for_attempt(next_attempt)
            @logger.info("Retrying http request, will sleep for #{sleep_for} seconds")
            timer_task = RetryTimerTask.new(pending, event, next_attempt)
            @timer.schedule(timer_task, sleep_for*1000)
          when :failure 
            failures.incrementAndGet
          else
            raise "Unknown action #{action}"
          end
          
          if action == :success || action == :failure 
            if successes.get+failures.get == @event_count
              pending << :done
            end
          end
        rescue => e 
          # This should never happen unless there's a flat out bug in the code
          @logger.error("Error sending HTTP Request",
            :class => e.class.name,
            :message => e.message,
            :backtrace => e.backtrace)
          failures.incrementAndGet
          raise e
        end
      end
    end
  end

  def multi_receive(events)
    return if events.empty?
    if @is_batch
        @event_count = 1
        @codec.encode(events)
    else
        @event_count = events.size
        events.each do |event|
            @codec.encode(event)
        end
    end
  end
  
  class RetryTimerTask < java.util.TimerTask
    def initialize(pending, event, attempt)
      @pending = pending
      @event = event
      @attempt = attempt
      super()
    end
    
    def run
      @pending << [@event, @attempt]
    end
  end

  def log_retryable_response(response)
    if (response.code == 429)
      @logger.debug? && @logger.debug("Encountered a 429 response, will retry. This is not serious, just flow control via HTTP")
    else
      @logger.warn("Encountered a retryable HTTP request in HTTP output, will retry", :code => response.code, :body => response.body)
    end
  end

  def log_error_response(response, url, event)
    log_failure(
              "Encountered non-2xx HTTP code #{response.code}",
              :response_code => response.code,
              :url => url,
              :data => event.to_hash,
              :event => event
            )
  end
  
  def sleep_for_attempt(attempt)
    sleep_for = attempt**2
    sleep_for = sleep_for <= 60 ? sleep_for : 60
    (sleep_for/2) + (rand(0..sleep_for)/2)
  end
  
  def send_event(event, data, attempt)
    # Send the request
    url = @is_batch ? @url : event.sprintf(@url)
    headers = @is_batch ? @headers : event_headers(event)

    # Compress the body and add appropriate header
    if @http_compression == true
      headers["Content-Encoding"] = "gzip"
      body = gzip(data)
    else
      body = data
    end

    # Create an async request
    request = client.background.send(@http_method, url, :body => body, :headers => headers)

    request.on_success do |response|
      begin
        if !response_success?(response)
          if retryable_response?(response)
            log_retryable_response(response)
            yield :retry, event, attempt
          else
            log_error_response(response, url, event)
            yield :failure, event, attempt
          end
        else
          yield :success, event, attempt
        end
      rescue => e 
        # Shouldn't ever happen
        @logger.error("Unexpected error in request success!",
          :class => e.class.name,
          :message => e.message,
          :backtrace => e.backtrace)
      end
    end

    request.on_failure do |exception|
      begin 
        will_retry = retryable_exception?(exception)
        log_failure("Could not fetch URL",
                    :url => url,
                    :method => @http_method,
                    :body => body,
                    :headers => headers,
                    :message => exception.message,
                    :class => exception.class.name,
                    :backtrace => exception.backtrace,
                    :will_retry => will_retry
        )
        
        if will_retry
          yield :retry, event, attempt
        else
          yield :failure, event, attempt
        end
      rescue => e 
        # Shouldn't ever happen
        @logger.error("Unexpected error in request failure!",
          :class => e.class.name,
          :message => e.message,
          :backtrace => e.backtrace)
        end
    end

    # Actually invoke the request in the background
    # Note: this must only be invoked after all handlers are defined, otherwise
    # those handlers are not guaranteed to be called!
    request.call 
  end

  def close
    @timer.cancel
    client.close
  end

  private
  
  def response_success?(response)
    code = response.code
    return true if @ignorable_codes && @ignorable_codes.include?(code)
    return code >= 200 && code <= 299
  end
  
  def retryable_response?(response)
    @retryable_codes.include?(response.code)
  end
  
  def retryable_exception?(exception)
    RETRYABLE_MANTICORE_EXCEPTIONS.any? {|me| exception.is_a?(me) }
  end

  # This is split into a separate method mostly to help testing
  def log_failure(message, opts)
    @logger.error("[HTTP Output Failure] #{message}", opts)
  end

  # gzip data
  def gzip(data)
    gz = StringIO.new
    gz.set_encoding("BINARY")
    z = Zlib::GzipWriter.new(gz)
    z.write(data)
    z.close
    gz.string
  end

  def convert_mapping(mapping, event)
    if mapping.is_a?(Hash)
      mapping.reduce({}) do |acc, kv|
        k, v = kv
        acc[k] = convert_mapping(v, event)
        acc
      end
    elsif mapping.is_a?(Array)
      mapping.map { |elem| convert_mapping(elem, event) }
    else
      event.sprintf(mapping)
    end
  end

  def map_event(event)
    if @mapping
      convert_mapping(@mapping, event)
    else
      event.to_hash
    end
  end

  def event_headers(event)
    custom_headers(event) || {}
  end

  def custom_headers(event)
    return nil unless @headers

    @headers.reduce({}) do |acc,kv|
      k,v = kv
      acc[k] = event.sprintf(v)
      acc
    end
  end

  def validate_format!
    unless @format.nil? or @format == "json"
      @logger.warn "The format parameter is deprecated. Use an appropriate codec instead."
    end
  end
end
