import pino from "pino";
import { getConfig } from "./config";
import createPulsarProducer from "./pulsar";
import createMqttClient from "./mqtt";

(async () => {
  const logger = pino({
    name: "mqtt-pulsar-forwarder",
    timestamp: pino.stdTimeFunctions.isoTime,
  });
  logger.info("Read configuration");
  const config = getConfig(logger);
  logger.info("Create Pulsar producer");
  const pulsarProducer = await createPulsarProducer(logger, config.pulsar);
  logger.info("Start MQTT subscriber");
  createMqttClient(logger, config.mqtt, pulsarProducer);
})();
