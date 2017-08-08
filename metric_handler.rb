#!/usr/bin/env ruby

if !defined?(Rails)
  ENV["RAILS_ROOT"] ||= File.expand_path("../manageiq", __dir__)
  require File.expand_path("config/environment", ENV["RAILS_ROOT"])
end

require "trollop"
require "manageiq-messaging"

Thread.abort_on_exception = true

def main(args)
  ManageIQ::Messaging.logger = Logger.new(STDOUT) if args[:debug]

  puts "Connecting..."
  ManageIQ::Messaging::Client.open(
    :host       => args[:q_hostname],
    :port       => args[:q_port],
    :username   => args[:q_user],
    :password   => args[:q_password],
    :client_ref => "event_handler",
  ) do |client|
    puts "Listening for events..."

    client.subscribe_messages(:service => "metrics", :limit => 10) do |messages|
      messages.each do |msg|
        payload = msg.payload

        ems_id = payload[:ems_id]
        metrics = payload[:metrics]
        save_metrics(ems_id, metrics)

        client.ack(msg.ack_ref)
      end
    end

    loop { sleep 5 }
  end
end

def save_metrics(id, metrics)
  # e = ExtmanagementSystem.find(payload[:ems_id])
  # o = e.vms.find_by(:ems_ref => payload[:ems_ref])
  # o.perf_process(interval_name, start_range, end_range, counters, counter_values)
  puts "ems[#{id}]: #{metrics.inspect}"
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

  # %i(q_hostname q_port q_user q_password).each do |param|
  #   raise Trollop::CommandlineError, "--#{param} required" if args[param].nil?
  # end

  args
end

args = parse_args

main args
