require "test_helper"

module Pulsar
  class ProducerTest < TestCase
    def test_producer_connection_error
      client = Client.new(service_url: "pulsar://invalid-hostname:6650")

      error = assert_raises(Pulsar::Error) do
        client.create_producer(topic: "persistent://public/default/foo", name: "bar")
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

      client = Client.new(service_url: "pulsar://localhost:6650")
      client.create_producer(topic: "persistent://public/default/foo", name: "bar") do |producer|
        block_called = true
        assert_equal("persistent://public/default/foo", producer.topic)
        assert_equal("bar", producer.name)

        producer.produce(data: "baz")
      end
      assert(block_called)
    end

    def test_producer_multiple_threads
      client = Client.new(service_url: "pulsar://localhost:6650")
      producer = client.create_producer(topic: "persistent://public/default/foo", name: "bar")

      begin
        3.times.map do
          Thread.new do
            producer.produce(data: "baz")
          end
        end.each(&:join)
      ensure
        producer.close
      end
    end

    def test_producer_multiple_processes
      skip("doesn't work")
      client = Client.new(service_url: "pulsar://localhost:6650")
      producer = client.create_producer(topic: "persistent://public/default/foo", name: "bar")
      begin
        Process.fork do
          producer.produce(data: "baz")
          exit(0)
        end
        Process.wait
      ensure
        producer.close
      end
    end
  end
end
