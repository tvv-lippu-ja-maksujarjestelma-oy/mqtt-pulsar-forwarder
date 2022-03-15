import mqtt from "async-mqtt";
import type pino from "pino";
import type Pulsar from "pulsar-client";
import type { MqttConfig } from "./config";
import transformUnknownToError from "./util";

const createMqttClientAndUnsubscribe = async (
  logger: pino.Logger,
  { url, topicFilter, clientOptions, subscribeOptions }: MqttConfig,
  pulsarProducer: Pulsar.Producer
) => {
  logger.info("Connect to MQTT broker");
  const client = await mqtt.connectAsync(url, clientOptions);

  const logIntervalInSeconds = 60;
  let nRecentPulsarMessages = 0;

  setInterval(() => {
    logger.info({ nRecentPulsarMessages }, "messages forwarded to Pulsar");
    nRecentPulsarMessages = 0;
  }, 1_000 * logIntervalInSeconds);

  // async-mqtt accepts only sync callbacks for receiving messages.
  client.on("message", (topic, message, packet) => {
    pulsarProducer
      .send({
        data: message,
        properties: {
          mqttTopic: topic,
          mqttQos: packet.qos.toFixed(0),
          mqttIsRetained: packet.retain.toString(),
          mqttIsDuplicate: packet.dup.toString(),
        },
        eventTimestamp: Date.now(),
      })
      .then(
        () => {
          nRecentPulsarMessages += 1;
        },
        (error) => {
          const err = transformUnknownToError(error);
          logger.fatal({ err }, "Publishing to Pulsar failed");
          // Aim to exit.
          throw error;
        }
      );
  });

  logger.info("Subscribe to MQTT topics");
  try {
    await client.subscribe(topicFilter, subscribeOptions);
  } catch (err) {
    logger.fatal({ err }, "Subscribing to MQTT topics failed");
    // Aim to exit.
    throw err;
  }

  const unsubscribe = async () => {
    try {
      return await client.unsubscribe(topicFilter);
    } catch (err) {
      // Aim to exit.
      throw transformUnknownToError(err);
    }
  };

  return { client, unsubscribe };
};

export default createMqttClientAndUnsubscribe;
