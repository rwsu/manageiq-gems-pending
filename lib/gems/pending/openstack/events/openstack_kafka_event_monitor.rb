require 'openstack/openstack_event_monitor'
require 'openstack/events/openstack_event'
require 'kafka'

# TODOS:
# 1. Match number of spawned OpenstackKafkaEventMonitors to number of
#    Kafka configured partitions.
# 2. Modify UI to accept seed broker URI as input and use value to
#    initialize Kafka client.
# 3. Modify UI to add Kakfa as a third option next to RabbitMQ and AMQP.
class OpenstackKafkaEventMonitor < OpenstackEventMonitor
  DEFAULT_SEED_BROKERS = ["localhost:9092"]
  DEFAULT_LOG_FILE = "log/kafka.log"

  def self.available?(options = {})
    connection_options = {}
    test_connection = nil
    begin
      if options.key? :seed_brokers
        connection_options[:seed_brokers] = options[:seed_brokers]
      else
        # TODO: else clause can be removed once UI entered values are being used
        connection_options[:seed_brokers] = DEFAULT_SEED_BROKERS
      end
      test_connection = Kafka.new(connection_options)
      true
    rescue => e
      log_prefix = "MIQ(#{self.class.name}.#{__method__}) Failed testing Kafka connection for #{options[:seed_brokers]}. "
      $log.debug("#{log_prefix} Exception: #{e}") if $log
      false
    ensure
      test_connection.close if test_connection.respond_to? :close
    end
  end

  def self.plugin_priority
    3
  end

  def connection(options = {})
    connection_options = {}
    $log.debug("MIQ(#{self.class.name}.#{__method__}) monitor #{self} options #{options}") if $log
    if options.key? :seed_brokers
      connection_options[:seed_brokers] = options[:seed_brokers]
    else
      # TODO: else clause can be removed once UI entered values are being used
      connection_options[:seed_brokers] = DEFAULT_SEED_BROKERS
    end
    if options.key? :logger
      connection_options[:logger] = Logger.new(options[:logger])
    else
      # TODO: else clause can be removed once UI entered values are being used
      connection_options[:logger] = Logger.new(DEFAULT_LOG_FILE)
    end
    @connection ||= Kafka.new(connection_options)
    @connection
  end

  def initialize(options = {})
    $log.debug("MIQ(#{self.class.name}.#{__method__}) monitor #{self} options #{options}") if $log
    @options = options
    @options[:seed_brokers] ||= DEFAULT_SEED_BROKERS
    @options[:logger] ||= DEFAULT_LOG_FILE
  end

  def start
    $log.debug("MIQ(#{self.class.name}.#{__method__}) connection #{@connection}") if $log
    @consumer = connection.consumer(group_id: "manageiq-consumer")
    connection.topics.each do |topic|
      @consumer.subscribe(topic) unless topic == "__consumer_offsets"
      num_partitions = connection.partitions_for(topic)
      $log.debug("MIQ(#{self.class.name}.#{__method__}) adding topic #{topic} num_partitions #{num_partitions}") if $log
    end
  end

  def stop
    $log.debug("MIQ(#{self.class.name}.#{__method__}) connection #{@connection}") if $log
    @consumer.stop
    @connection.close
  end

  def each_batch
    begin
      @consumer.each_batch do |batch|
        $log.debug("MIQ(#{self.class.name}.#{__method__}) received topic #{batch.topic} partition #{batch.partition}") if $log
        kafka_events = batch.messages.map do |message|
          # TODO: Next lines needs to be adjusted when we know what the Kafka messages look like
          $log.debug("MIQ(#{self.class.name}.#{__method__}) offset #{message.offset} key #{message.key} value #{message.value} monitor #{self} connection #{@connection}") if $log
          metadata = {:user_id => nil, :priority => nil, :content_type => nil}
          payload = {
              "message_id" => message.offset,
              "event_type" => message.topic,
              "timestamp"  => Time.now,
              "payload"    => { "payload" => message.value },
          }
          openstack_event(nil, metadata, payload)
        end
        yield kafka_events
      end
    rescue => e
      log_prefix = "MIQ(#{self.class.name}.#{__method__}) Failed each_batch for connection #{@connection}. "
      $log.debug("#{log_prefix} Exception: #{e}") if $log
    end
  end
end

