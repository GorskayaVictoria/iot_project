Sensor.create(
  name: "HUMIDITY",
  number: "FIRST",
  value_type: "PERCENT",
  value: "0"
) unless Sensor.where(name: "HUMIDITY").exists?

Sensor.create(
  name: "MOTION",
  number: "SECOND",
  value_type: "ANGLE",
  value: "0"
) unless Sensor.where(name: "MOTION").exists?

Sensor.create(
  name: "LED",
  number: "THIRD",
  value_type: "COLOR",
  value: "0-0-0"
) unless Sensor.where(name: "LED").exists?

Sensor.create(
  name: "TEMPERATURE",
  number: "FOURTH",
  value_type: "DEGREES",
  value: "0"
) unless Sensor.where(name: "TEMPERATURE").exists?
