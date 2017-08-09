#!/usr/bin/env ruby

if !defined?(Rails)
  ENV["RAILS_ROOT"] ||= File.expand_path("../manageiq", __dir__)
  require File.expand_path("config/environment", ENV["RAILS_ROOT"])
end

require "trollop"
require "manageiq-messaging"

Thread.abort_on_exception = true

class MetricHandler
  def initialize
    @options = {}
  end

  def subscribe_messages
    ManageIQ::Messaging.logger = Logger.new(STDOUT) if @options[:debug]

    puts "Connecting..."
    ManageIQ::Messaging::Client.open(
      :host       => @options[:q_hostname],
      :port       => @options[:q_port],
      :username   => @options[:q_user],
      :password   => @options[:q_password],
      :client_ref => "event_handler"
    ) do |client|
      puts "Listening for events..."
      client.subscribe_messages(:service => "metrics", :limit => 10) do |messages|
        messages.each do |msg|
          start_time = Time.now
          payload = msg.payload

          ems_id         = payload[:ems_id]
          ems_ref        = payload[:ems_ref]
          
          if (obj = find_object(ems_id, ems_ref))
            interval_name  = payload[:interval_name]
            counters       = payload[:counters]
            counter_values = payload[:counter_values]
            start_range, end_range = determine_range(payload[:start_range], payload[:end_range], counter_values)

            print "metrics ems[#{"%3d" % ems_id}].vms[#{ "%5s" % ems_ref}] -- #{interval_name} #{start_range} - #{end_range}"
            obj.perf_process(interval_name, start_range, end_range, counters, counter_values)
            end_time = Time.now
            print " [took: #{end_time - start_time}]\n"
          else
            puts "metrics ems[#{ems_id}].vms[#{ems_ref}] -- not found"
          end

          client.ack(msg.ack_ref)
        end
      end
      loop { sleep 5 }
    end

    self
  end

  def parse
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

    @options = args
    self
  end

  def determine_range(start_range, end_range, counter_values)
    if start_range.nil? || end_range.nil?
      dates = counter_values.keys.sort
      start_range ||= dates.first
      end_range ||= dates.last
    end

    [start_range, end_range]
  end

  def find_object(ems_id, ems_ref)
    # ems = ExtManagementSystem.find(ems_id)
    obj_type = ems_ref.split("-").first
    case obj_type
    when "vm"
      Vm.find_by(:ems_id => ems_id, :ems_ref => ems_ref)
    end
  end
end

MetricHandler.new.parse.subscribe_messages
