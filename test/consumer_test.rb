require "test_helper"

module Pulsar
  class ConsumerTest < TestCase
    def test_consumer_connection_error
      client = Client.new(service_url: "pulsar://invalid-hostname:6650")

      error = assert_raises(Pulsar::Error) do
        client.create_consumer(
          topic: "my-topic-#{Time.now.to_i}",
          subscription: "my-sub",
        )
      end
      assert(error.message.end_with?("ConnectError"))
    end

    def test_consumer
      block_called = false

      client = Client.new(service_url: "pulsar://localhost:6650")

      topic = "my-topic-#{Time.now.to_i}"

      client.create_producer(topic: topic) do |producer|
        client.create_consumer(topic: topic, subscription: "my-sub") do |consumer|
          block_called = true

          3.times.each do |i|
            send_time = Time.now
            producer.produce(data: "hello-#{i}")

            message = consumer.receive
            receive_time = Time.now

            puts("send_time: #{send_time}")
            puts("receive_time: #{receive_time}")
            puts("publish_time: #{message.publish_time}")

            assert_equal("hello-#{i}", message.data)
            assert_equal("persistent://public/default/#{topic}", message.topic)
            assert(send_time <= message.publish_time)
            assert(receive_time >= message.publish_time)

            consumer.ack(message)
          end

          assert_equal("persistent://public/default/#{topic}", consumer.topic)
          assert_equal("my-sub", consumer.subscription)
        end
      end

      assert(block_called)
    end
  end
end
