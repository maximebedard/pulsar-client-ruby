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
        client.create_consumer(
          topic: topic,
          subscription: "my-sub",
          unacked_message_timeout_ms: 1_000_000,
          name: "my-consumer",
          receiver_queue_size: 100,
          max_total_receiver_queue_size_across_partitions: 10000,
          type: :shared,
        ) do |consumer|
          block_called = true

          3.times.each do |i|
            send_time = Time.now
            producer.produce(
              data: "hello-#{i}"
            )

            msg = consumer.receive
            receive_time = Time.now

            # puts("send_time: #{send_time}")
            # puts("receive_time: #{receive_time}")
            # puts("publish_time: #{msg.publish_time}")

            assert_equal("hello-#{i}", msg.data)
            assert_equal("persistent://public/default/#{topic}", msg.topic)
            assert_equal(0, msg.event_timestamp)
            assert_equal(Time.at(0), msg.event_time)
            assert_nil(msg.partition_key)
            assert_nil(msg.ordering_key)
            # assert(send_time <= msg.publish_time)
            # assert(receive_time >= msg.publish_time)

            consumer.ack(msg)
          end

          assert_equal("persistent://public/default/#{topic}", consumer.topic)
          assert_equal("my-sub", consumer.subscription)
        end
      end

      assert(block_called)
    end

    def test_consumer_seek
    end

    def test_consumer_initial_position
    end

    def test_consumer_nack
    end

    def test_consumer_shared
    end

    def test_consumer_unacked_message_timeout
    end
  end
end
