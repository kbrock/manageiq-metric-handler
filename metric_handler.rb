#!/usr/bin/env ruby

Thread.abort_on_exception = true

class MetricsHandler
  attr_accessor :exit_requested
  def initialize(options)
    @options = options
    @exit_requested = false
  end

  def run
    client

    log.info "Listening for metrics..."
    client.subscribe_messages(:service => "metrics", :limit => 10) do |messages|
      messages.each do |msg|
        process_message(msg)
        client.ack(msg.ack_ref)
      end
      if @exit_requested
        close
        break
      end
    end

    self
  end

  def process_message(msg)
    start_time = Time.now
    payload = msg.payload

    ems_id = payload[:ems_id]
    ems_ref = payload[:ems_ref]
    klass = payload[:ems_klass]
    
    obj = find_object(klass, ems_id, ems_ref)
    if obj.nil?
      puts "metrics ems[#{"%3d" % ems_id}].vms[#{ "%9s" % ems_ref}] -- not found"
      return
    end

    interval_name  = payload[:interval_name]
    counters       = payload[:counters]
    counter_values = payload[:counter_values]
    start_range    = payload[:start_range] || counter_values.keys.min
    end_range      = payload[:end_range]   || counter_values.keys.max


    print "metrics ems[#{"%3d" % ems_id}].vms[#{ "%9s" % ems_ref}] -- #{interval_name} #{start_range} - #{end_range}"
    obj.perf_process(interval_name, start_range, end_range, counters, counter_values)
    end_time = Time.now
    print " [took: #{end_time - start_time}]\n"
  end

  def close
    log.info("Closing connection") if @clienta
    @client.close if @client
    @client = nil
  end

  def stop
    log.info("Exit requested...")
    @exit_requested = true
  end

  private

  def determine_range(start_range, end_range, counter_values)
    if start_range.nil? || end_range.nil?
    end

    [start_range, end_range]
  end

  def find_object(klass, ems_id, ems_ref)
    klass.constantize.find_by(:ems_id => ems_id, :ems_ref => ems_ref)
  end

  def client
    @client ||= connect
  end

  def connect
    log.info "Connecting..."
    ManageIQ::Messaging::Client.open(
      :host       => @options[:q_hostname],
      :port       => @options[:q_port],
      :username   => @options[:q_user],
      :password   => @options[:q_password],
      :client_ref => "event_handler"
    ).tap { log.info "Connected" }
  end

  def log
    @logger ||= Logger.new(STDOUT)
  end
end

# basically the worker.rb portion

if !defined?(Rails)
  ENV["RAILS_ROOT"] ||= File.expand_path("../manageiq", __dir__)
  require File.expand_path("config/environment", ENV["RAILS_ROOT"])
end

require "trollop"
require "manageiq-messaging"

def main(args)
  ManageIQ::Messaging.logger = Logger.new(STDOUT) if args[:debug]

  handler = MetricsHandler.new(args)
  thread = Thread.new { handler.run }

  begin
    loop { sleep 1 }
  rescue Interrupt
    handler.stop
    thread.join
  end
end

def parse_args
  args = Trollop.options do
    opt :q_hostname, "queue hostname", :type => :string
    opt :q_port,     "queue port",     :type => :integer
    opt :q_user,     "queue username", :type => :string
    opt :q_password, "queue password", :type => :string
    opt :debug,      "debug", :type => :flag
  end

  args[:q_hostname]   ||= ENV["QUEUE_HOSTNAME"] || "localhost"
  args[:q_port]       ||= ENV["QUEUE_PORT"]     || "61616"
  args[:q_user]       ||= ENV["QUEUE_USER"]     || "admin"
  args[:q_password]   ||= ENV["QUEUE_PASSWORD"] || "smartvm"

  args[:q_port] = args[:q_port].to_i

  missing = %i(q_hostname q_port q_user q_password).select { |param| args[param].nil? }
  raise Trollop::CommandlineError, "#{missing.map { |p| "--#{p} VAL"}.join(" ")} required" unless missing.empty?

  args
end

args = parse_args

main args
