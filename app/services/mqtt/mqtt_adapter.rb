module Mqtt
  class MqttAdapter
    include Singleton

    def start_session
      client_subscribe = client.subscribe("team11/sensors_data/primary")
      loop do
        client.get do |topic,message|
          sensors_data = JSON.parse(message).deep_symbolize_keys[:sensors_data]
          sensors_data.each do |sensor_data|
            sensor = Sensor.find_by(name: sensor_data[:name])
            sensor.update(value: sensor_data[:value]) if sensor.present?
            sensor.update(updated_at: Time.zone.now) if sensor.present?

          end
        end
      end

    end

    def restart_session
      stop_session
      start_session
    end

    def stop_session
      @session_thread.kill
      @session_thread = nil
    end

    def send_sensor_data(sensor)
      return if sensor.blank?

      payload = {name: sensor.name, value: sensor.value}.to_json
      topic = "team11/command/primary"

      client.publish(topic, payload)
    end

    private

    def client
      @client ||= client = MQTT::Client.connect(
        :host => 'broker.hivemq.com',
        :port => 1883
      )
    end
  end
end
