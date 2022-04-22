class SensorsController < ApplicationController
  def index
    render json: sensor_data
  end

  private

  def sensor_data
    sensors.map do |sensor|
      {
        "sensorId" => sensor.number,
        "sensorType" => sensor.name,
        "messageType" => "INFO",
        "typeValue" => sensor.value_type,
        "value" => sensor.value
      }
    end
  end

  def sensors
    Sensor.all
  end
end
