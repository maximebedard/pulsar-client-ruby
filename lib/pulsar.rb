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
      from_utc_timestamp(publish_timestamp)
    end

    def event_time
      from_utc_timestamp(event_timestamp)
    end

    private

    def from_utc_timestamp(timestamp)
      ms = timestamp * 1000000
      s = ms / 1000000000
      ns = ms - (s * 1000000000)
      Time.at(s, ns, :nsec)
    end
  end
end
