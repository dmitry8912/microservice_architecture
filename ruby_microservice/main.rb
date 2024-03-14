require 'bunny'
require 'logger'
require 'json'
require 'mongo'
require 'securerandom'

logger = Logger.new(STDOUT)
logger.level = Logger::DEBUG

connection = Bunny.new(ENV['RABBITMQ_URL'], automatically_recover: false)
connection.start
logger.info("AMQP connected")

channel = connection.create_channel
logger.info("Channel created")

queue_name = ENV["SERVICE_NAME"]

queue = channel.queue("#{queue_name}_requests", {
  durable: true,
  auto_delete: false,
  exclusive: false
})

exchange = channel.default_exchange

mongo_client = Mongo::Client.new(ENV["MONGO_DSN"])
sc_client = mongo_client.use('services_configs')
collection = sc_client[:services_configs]
config = collection.find("service": ENV["SERVICE_NAME"]).first
logger.info("Config #{config}")

begin
  logger.info("Queue consumer started")
  # block: true is only used to keep the main thread
  # alive. Please avoid using it in real world applications.
  queue.subscribe(block: true) do |_delivery_info, properties, body|
    payload = JSON.parse(body.to_s)
    logger.info("Incoming message id #{properties.correlation_id} #{payload}")
    result = {
      "service_name": ENV['SERVICE_NAME'],
      result: {
        "status": "success",
        "data": SecureRandom.uuid.to_s,
        "request": payload,
        "start_at": Time.now.to_s,
        "end_at": Time.now.to_s
      }
    }
    exchange.publish(
      JSON.dump(result),
      routing_key: "#{config[:next_service]}_requests",
      correlation_id: properties.correlation_id
    )
    logger.info("Message processed and sended to #{config[:next_service]}")
  end
rescue Interrupt => _
  connection.close

  exit(0)
end