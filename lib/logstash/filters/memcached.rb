# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# This  filter will replace the contents of the default
# message field with whatever you specify in the configuration.
#
# It is only intended to be used as an .
class LogStash::Filters::Memcached < LogStash::Filters::Base

  # Setting the config_name here is required. This is how you
  # configure this filter from your Logstash config.
  #
  # memcached {
  #    {
  #     host => "localhost"
  #     port => 11211
  #     key => "memcache_key"
  #     get => true
  #     field => "my_field"
  #   }
  # }
  #
  config_name "memcached"

  # Memcached host
  config :host, :validate => :string, :default => "localhost", :required => true

  # Memcached port
  config :port, :validate => :number, :default => 11211, :required => true

  # Memcached key
  config :key, :validate => :string, :required => true

  # Get or set. If this is true, will get from memcached, if false, will set.
  config :get, :validate => :boolean, :default => true

  # :field is to be used in the case of a non-JSON result.
  # It is the field that should be retrieved or set in memcached.
  # This is the most basic functionality.
  config :field, :validate => :string, :default => ""

  # :fields is to be used in the case of a JSON result (ex. an Elasticsearch log)
  # This is the fancier use case.
  # It also has some rename functionality.
  #
  # GET:
  # In the case you are expecting a JSON result, these are the fields that will
  # be copied from that JSON result into the new event.
  # The key in the hash is the field name in the memcached result and the value
  # is the field name that will be used in the new event.
  # To prevent verbosity, if the field names are the same, the value should be
  # nil. (ex. {"My Field" => nil})
  # Format: {"Old Field 1" => "New Field 1", "Old Field 2" => "New Field 2"}
  #
  # SET:
  # These fields from the event will be stored in a JSON object in memcached.
  # Format: {"Field 1" => nil, "Field 2" => nil}
  config :json_fields, :validate => :hash, :default => {}


  public
  def register
    require 'dalli'
    require 'json'
    options = {}
    @memcached = Dalli::Client.new("#{@host}:#{@port}", options)
  end # def register

  public
  def filter(event)
    # We're getting from memcache!
    if @get
      data = @memcached.get(@key)
      if !@json_fields.empty?
        json = JSON.parse(data)
        @json_fields.each do |old_field, new_field|
          if json.include? old_field
            event.set(new_field, json[old_field])
          end
        end
      else
        event.set(@field, data)
      end
    # We're setting in memcache!
    else
      if !@json_fields.empty?
        result = {}
        @json_fields.each do |field|
          result[field] = event.get(field)
        end
        @memcached.set(@key, result.to_json)
      else
        @memcached.set(@key, event.get(@field))
      end
    end
    # filter_matched should go in the last line of our successful code
    filter_matched(event)
  end # def filter
end # class LogStash::Filters::Memcached
