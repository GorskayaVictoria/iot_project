class SendSensorsDataToKafka
  include Sidekiq::Worker

  def perform
    Sensor.all.each { |sensor| kafka_adapter.produce_sensor_data(sensor) }
  end

  private

  def kafka_adapter
    @kafka_adapter ||= Kafka::Adapter.instance
  end
end
