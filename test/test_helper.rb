$LOAD_PATH.unshift File.expand_path("../../lib", __FILE__)
require "pulsar"
require "pry-byebug"
require "minitest/autorun"

class TestCase < Minitest::Test
end
