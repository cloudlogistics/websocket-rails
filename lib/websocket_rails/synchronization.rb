require "redis"

module WebsocketRails
  class Synchronization

    def self.all_users
      singleton.all_users
    end

    def self.find_user(connection)
      singleton.find_user connection
    end

    def self.register_user(connection)
      singleton.register_user connection
    end

    def self.destroy_user(connection)
      singleton.destroy_user connection
    end

    def self.publish(event)
      singleton.publish event
    end

    def self.synchronize!
      singleton.synchronize!
    end

    def self.shutdown!
      singleton.shutdown!
    end

    def self.redis
      singleton.redis
    end

    def self.singleton
      @singleton ||= new
    end

    include Logging

    def redis
      @redis ||= begin
        if defined?(EM) && EM.reactor_running?
          em_redis
        else
          ruby_redis
        end
      end
    end

    # Redis connection for EventMachine environments (Thin).
    # Uses the synchrony driver when em-synchrony is available so Redis calls
    # yield to the EM reactor instead of blocking it.
    def em_redis
      @em_redis ||= begin
        options = WebsocketRails.config.redis_options.dup
        if defined?(EM::Synchrony)
          options[:driver] = :synchrony
        end
        Redis.new(options)
      end
    end

    def ruby_redis
      @ruby_redis ||= begin
        Redis.new(WebsocketRails.config.redis_options)
      end
    end

    # Dedicated Redis connection for the synchronization subscriber thread's
    # control commands (server-token set membership). Built directly from
    # config.redis_options so it is NOT affected by host apps that override
    # #redis / #ruby_redis to return a shared connection (or a bare options
    # Hash). Keeping it private to the subscriber thread also avoids sharing a
    # single connection across threads, which the redis client does not support.
    def sync_control_redis
      @sync_control_redis ||= Redis.new(WebsocketRails.config.redis_options)
    end

    def publish(event)
      Fiber.new do
        Rails.logger.info '*' * 100
        Rails.logger.info 'Publishing event'
        Rails.logger.info '*' * 100
        event.server_token = server_token

        # The method is overridden in websocket-rails initializer to support
        # redis in EM initialization. EM requires the configuration instead of redis
        # instance when initializing websocket-rails
        instantiated_redis = redis.is_a?(Hash) ? Redis.new(redis) : redis
        instantiated_redis.publish "websocket_rails.events", event.serialize
      end.resume
    end

    def server_token
      @server_token
    end

    def synchronize!
      # PID-guarded rather than a simple boolean: under Puma clustered mode the
      # subscriber thread does not survive fork, so each worker process must be
      # able to (re)start its own subscriber even though the singleton (and this
      # flag) were inherited from the preloaded master.
      return if @synchronizing_pid == Process.pid
      @synchronizing_pid = Process.pid

      # Only take over process signal handling in the dedicated standalone
      # websocket server, which owns its process. When websocket-rails is
      # embedded in the main app (Thin) or running under Puma, the host server
      # installs its own TERM/INT/QUIT handlers for graceful shutdown, so we
      # must not clobber them; an at_exit cleanup is sufficient there.
      if WebsocketRails.standalone?
        trap('TERM') { Thread.new { shutdown! } }
        trap('INT')  { Thread.new { shutdown! } }
        trap('QUIT') { Thread.new { shutdown! } }
      else
        at_exit { shutdown! rescue nil }
      end

      @server_token = generate_server_token
      register_server(@server_token)

      # This method always runs inside a dedicated thread (see
      # ConnectionManager#ensure_thread_synchronization), so the blocking Redis
      # SUBSCRIBE below does NOT block the EventMachine reactor / web server.
      # A subscribed connection cannot issue other commands, so it needs its own
      # dedicated connection, separate from the one used for publishing.
      subscriber_redis = Redis.new(WebsocketRails.config.redis_options)
      info "Beginning Synchronization"
      subscriber_redis.subscribe "websocket_rails.events" do |on|
        on.message do |_, encoded_event|
          dispatch_incoming(encoded_event)
        end
      end
    end

    # Dispatch an event received from Redis to the local connections. Socket
    # writes for faye-websocket connections under Thin must happen on the
    # EventMachine reactor thread, so when a reactor is running we hop back onto
    # it via EM.next_tick (thread-safe). Under Puma there is no reactor, so we
    # handle it inline in the subscriber thread.
    def dispatch_incoming(encoded_event)
      if defined?(EM) && EM.reactor_running?
        EM.next_tick { handle_incoming(encoded_event) }
      else
        handle_incoming(encoded_event)
      end
    end

    def handle_incoming(encoded_event)
      event = Event.new_from_json(encoded_event, nil)

      # Do nothing if this is the server that sent this event.
      return if event.server_token == server_token

      # Ensure an event never gets triggered twice. Events added to the redis
      # queue from other processes may not have a server token attached.
      event.server_token = server_token if event.server_token.nil?

      trigger_incoming event
    end

    def trigger_incoming(event)
      case
      when event.is_channel?
        WebsocketRails[event.channel].trigger_event(event)
      when event.is_user?
        connection = WebsocketRails.users[event.user_id.to_s]
        return if connection.nil?
        connection.trigger event
      end
    end

    def shutdown!
      remove_server(server_token)
    end

    def generate_server_token
      begin
        token = SecureRandom.urlsafe_base64
      end while sync_control_redis.sismember("websocket_rails.active_servers", token)

      token
    end

    def register_server(token)
      # sadd? avoids the Redis 5 deprecation warning for the boolean-returning
      # sadd; the return value is unused here. Runs in the subscriber thread on a
      # dedicated (blocking) connection.
      sync_control_redis.sadd? "websocket_rails.active_servers", token
      info "Server Registered: #{token}"
    end

    def remove_server(token)
      # srem? avoids the Redis 5 deprecation warning for the boolean-returning
      # srem; the return value is unused here.
      sync_control_redis.srem? "websocket_rails.active_servers", token
      info "Server Removed: #{token}"
      # Only the dedicated standalone websocket server owns its reactor and may
      # stop it. When embedded in the main app (Thin), stopping the reactor here
      # would take down the whole web process, so leave it running.
      EM.stop if WebsocketRails.standalone? && defined?(EM) && EM.reactor_running?
    end

    def register_user(connection)
      Fiber.new do
        id = connection.user_identifier
        user = connection.user
        redis.hset 'websocket_rails.users', id, user.as_json(root: false).to_json
      end.resume
    end

    def destroy_user(identifier)
      Fiber.new do
        redis.hdel 'websocket_rails.users', identifier
      end.resume
    end

    def find_user(identifier)
      Fiber.new do
        raw_user = redis.hget('websocket_rails.users', identifier)
        raw_user ? JSON.parse(raw_user) : nil
      end.resume
    end

    def all_users
      Fiber.new do
        redis.hgetall('websocket_rails.users')
      end.resume
    end

  end
end
