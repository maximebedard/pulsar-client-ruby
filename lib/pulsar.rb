require "pulsar/version"
require "pulsar/pulsar"

module Pulsar
  class Client
    def create_producer(**options)
      Producer.new(self, **options)
    end

    def create_consumer(**options)
      Consumer.new(self, **options)
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

  class ConsumerMessage
    def initialize(id: nil)
      @id = id

      freeze
    end

    attr_reader(:id)
  end

  class ProducerMessage
    def initialize(
      key: nil,
      payload: nil,
      properties: nil,
      timestamp: 0,
      sequence_id: 0
    )
      @key = key
      @payload = payload
      @properties = properties
      @timestamp = timestamp
      @sequence_id = sequence_id

      freeze
    end

    attr_reader(
      :key,
      :payload,
      :properties,
      :timestamp,
      :sequence_id,
    )
  end
end
