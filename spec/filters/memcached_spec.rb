# encoding: utf-8
require_relative '../spec_helper'
require "logstash/filters/memcached"

describe LogStash::Filters::Memcached do
  describe "Set to Hello World" do
    let(:config) do <<-CONFIG
      filter {
        memcached {
          message => "Hello World"
        }
      }
    CONFIG
    end

    sample("message" => "some text") do
      expect(subject).to include("message")
      expect(subject['message']).to eq('Hello World')
    end
  end
end
