import mqtt from "mqtt";
import pino from "pino";
import Pulsar from "pulsar-client";
import { MqttConfig } from "./config";

const createMqttClient = (
  logger: pino.Logger,
  { url, topicFilter, clientOptions, subscribeOptions }: MqttConfig,
  pulsarProducer: Pulsar.Producer
) => {
  logger.info("Connect to MQTT broker");
  const mqttClient = mqtt.connect(url, clientOptions);

  mqttClient.on("message", async (topic, message, packet) => {
    await pulsarProducer.send({
      data: message,
      properties: {
        mqttTopic: topic,
        mqttQos: packet.qos.toFixed(0),
        mqttIsRetained: packet.retain.toString(),
        mqttIsDuplicate: packet.dup.toString(),
      },
      eventTimestamp: Date.now(),
    });
  });

  mqttClient.on("connect", () => {
    logger.info("Subscribe to MQTT topics");
    mqttClient.subscribe(topicFilter, subscribeOptions, (err) => {
      if (err) {
        logger.error({ err }, "Subscribing to MQTT topics failed");
      }
    });
  });

  return mqttClient;
};

export default createMqttClient;
