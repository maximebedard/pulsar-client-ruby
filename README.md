# pulsar-client-ruby

unofficial ruby wrapper for apache pulsar.

Supports:
- [x] Producer
  - [ ] schema
  - [ ] produce_async
- [x] Consumer
  - [ ] schema
  - [ ] consume_async
- [ ] Reader

## Installation

Since it's built using `libpulsar`, the [shared library and the headers needs to be installed](https://pulsar.apache.org/docs/en/client-libraries-cpp/).

```ruby
gem 'pulsar-client-ruby'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install pulsar-client-ruby

## Usage

Producer:

```rb
client = Client.new(service_url: "pulsar://localhost:6650")
producer = client.create_producer(
  topic: "persistent://public/default/foo",
  name: "bar",
)

producer.produce(Message.new(data: "foo"))
```

Consumer:

```rb
client = Client.new(service_url: "pulsar://localhost:6650")
consumer = client.create_consumer(
  topic: "persistent://public/default/foo",
  subscription: "bar",
)

loop do
  message = consumer.receive # will block
  puts(message.data)
  consumer.ack(message)
end
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/pulsar-client-ruby.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
