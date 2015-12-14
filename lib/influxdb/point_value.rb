module InfluxDB
  # Convert data point to string using Line protocol
  class PointValue
    attr_reader :series, :values, :tags, :timestamp

    def initialize(data)
      @series    = data[:series].gsub(/\s/, '\ ').gsub(',', '\,')
      @values    = data_to_string(data[:values], true, true)
      @tags      = data_to_string(data[:tags])
      @timestamp = data[:timestamp]
    end

    def dump
      dump = "#{@series}"
      dump << ",#{@tags}" if @tags
      dump << " #{@values}"
      dump << " #{@timestamp}" if @timestamp
      dump
    end

    private

    def data_to_string(data, quote_escape = false, may_have_integers = false)
      return nil unless data && !data.empty?
      mappings = map(data, quote_escape, may_have_integers)
      mappings.join(',')
    end

    def map(data, quote_escape, may_have_integers)
      data.map do |k, v|
        key = escape_key(k)

        if v.is_a?(String)
          val = escape_value(v, quote_escape)
        elsif may_have_integers && v.is_a?(Fixnum)
          val = "#{v}i"
        else
          val = v
        end

        "#{key}=#{val}"
      end
    end

    def escape_value(value, quote_escape)
      val = value.
        gsub(/\s/, '\ ').
        gsub(',', '\,').
        gsub('"', '\"')
      val = %("#{val}") if quote_escape
      val
    end

    def escape_key(key)
      key.to_s.gsub(/\s/, '\ ').gsub(',', '\,')
    end
  end
end
