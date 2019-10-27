require "test_helper"

module Pulsar
  class ConsumerTest < TestCase
    def test_consumer
      block_called = false

      client = Client.new(service_url: "pulsar://localhost:6650")
      client.create_consumer(topic: "persistent://public/default/foo", subscription: "bar") do |consumer|
        block_called = true
        assert_equal("persistent://public/default/foo", consumer.topic)
        assert_equal("bar", consumer.subscription)

        message = consumer.receive
        consumer.ack(message)
      end
      assert(block_called)
    end
  end
end
