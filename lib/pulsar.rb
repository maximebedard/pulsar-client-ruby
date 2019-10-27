require "pulsar/version"
require "pulsar/pulsar"

module Pulsar
  class Client
    def create_producer(**options)
      producer = Producer.new(self, **options)
      if block_given?
        begin
          yield producer
        ensure
          producer.close
        end
      else
        producer
      end
    end

    def create_consumer(**options)
      consumer = Consumer.new(self, **options)
      if block_given?
        begin
          yield consumer
        ensure
          consumer.close
        end
      else
        consumer
      end
    end
  end

  private_constant(:Producer)
  private_constant(:Consumer)

  class Message
    def publish_time
      Time.at(publish_timestamp)
    end

    def event_time
      Time.at(event_timestamp)
    end
  end
end
