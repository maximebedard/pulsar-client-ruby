require "test_helper"

module Pulsar
  class ConsumerTest < Minitest::Test
    def test_consumer
      client = Client.new(service_url: "pulsar://localhost:6650")
      consumer = client.create_consumer(topic: "persistent://public/default/foo", subscription: "bar")

      assert_equal("persistent://public/default/foo", consumer.topic)
      assert_equal("bar", consumer.subscription)
    end
  end
end
