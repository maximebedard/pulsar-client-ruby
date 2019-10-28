require "test_helper"
require "logger"

module Pulsar
  class ProducerTest < TestCase
    def test_producer_logger
      skip("doesn't work")

      logger = Logger.new(StringIO.new)
      client = Client.new(
        service_url: "pulsar://localhost:6650",
        logger_proc: -> (_level, _file, _line, _message) do
          logger.info("foo")
        end
      )

      client.create_producer(
        topic: "my-topic-#{Time.now.to_i}",
        name: "my-producer",
      ) do |producer|
        producer.produce(data: "WEWER")
      end
    end

    def test_producer_connection_error
      client = Client.new(service_url: "pulsar://invalid-hostname:6650")

      error = assert_raises(Pulsar::Error) do
        client.create_producer(
          topic: "my-topic-#{Time.now.to_i}",
          name: "my-producer",
        )
      end
      assert(error.message.end_with?("ConnectError"))
    end

    def test_producer_missing_topic
      client = Client.new(service_url: "pulsar://localhost:6650")

      error = assert_raises(KeyError) do
        client.create_producer
      end
      assert_equal(:topic, error.key)
    end

    def test_producer
      block_called = false

      topic = "my-topic-#{Time.now.to_i}"

      client = Client.new(service_url: "pulsar://localhost:6650")
      client.create_producer(
        topic: topic,
        name: "my-producer",
      ) do |producer|
        block_called = true
        assert_equal("persistent://public/default/#{topic}", producer.topic)
        assert_equal("my-producer", producer.name)
        assert_equal(-1, producer.last_sequence_id)

        producer.produce(
          event_timestamp: Time.now.to_i * 1000,
          sequence_id: 12,
          partition_key: "foo",
          ordering_key: "bar",
          data: "baz",
        )

        assert_equal(12, producer.last_sequence_id)
      end
      assert(block_called)
    end

    def test_producer_message_router
    end

    def test_producer_flush
    end

    def test_producer_batch
    end

    def test_producer_multiple_threads
      client = Client.new(service_url: "pulsar://localhost:6650")
      client.create_producer(
        topic: "my-topic-#{Time.now.to_i}",
        name: "my-producer",
      ) do |producer|
        3.times.map do
          Thread.new do
            producer.produce(data: "baz")
          end
        end.each(&:join)
      end
    end

    def test_producer_multiple_processes
      skip("doesn't work")

      client = Client.new(service_url: "pulsar://localhost:6650")
      client.create_producer(
        topic: "my-topic-#{Time.now.to_i}",
        name: "my-producer",
      ) do |producer|
        Process.fork do
          producer.produce(data: "baz")
          exit(0)
        end
        Process.wait
      end
    end
  end
end
