require "test_helper"

module Pulsar
  class ProducerTest < Minitest::Test
    def test_producer
      client = Client.new(service_url: "pulsar://localhost:6650")
      producer = client.create_producer(topic: "persistent://public/default/foo", name: "bar")

      assert_equal("persistent://public/default/foo", producer.topic)
      assert_equal("bar", producer.name)

      producer.produce(Message.new(data: "baz"))
    end
  end
end
