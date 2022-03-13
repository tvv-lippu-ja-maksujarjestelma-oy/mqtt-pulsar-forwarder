import type mqtt from "async-mqtt";
import pino from "pino";
import type Pulsar from "pulsar-client";
import { getConfig } from "./config";
import createHealthCheckServer from "./healthCheck";
import createMqttClientAndUnsubscribe from "./mqtt";
import { createPulsarClient, createPulsarProducer } from "./pulsar";
import transformUnknownToError from "./util";

/**
 * Exit gracefully.
 */
const exitGracefully = async (
  logger: pino.Logger,
  exitError: Error,
  exitCode: number,
  setHealthOk?: (isOk: boolean) => void,
  mqttClient?: mqtt.AsyncMqttClient,
  mqttUnsubscribe?: () => Promise<void>,
  pulsarClient?: Pulsar.Client,
  pulsarProducer?: Pulsar.Producer
) => {
  logger.fatal(exitError);
  logger.info("Start exiting gracefully");
  process.exitCode = exitCode;
  try {
    if (setHealthOk) {
      logger.info("Set health checks to fail");
      setHealthOk(false);
    }
  } catch (err) {
    logger.error(
      { err },
      "Something went wrong when setting health checks to fail"
    );
  }
  try {
    if (mqttUnsubscribe) {
      logger.info("Unsubscribe from MQTT topics");
      await mqttUnsubscribe();
    }
    if (mqttClient) {
      logger.info("Disconnect from MQTT broker");
      await mqttClient.end();
    }
  } catch (err) {
    logger.error({ err }, "Something went wrong when closing MQTT client");
  }
  try {
    if (pulsarProducer) {
      logger.info("Flush Pulsar producer");
      await pulsarProducer.flush();
      logger.info("Close Pulsar producer");
      await pulsarProducer.close();
    }
    if (pulsarClient) {
      logger.info("Close Pulsar client");
      await pulsarClient.close();
    }
  } catch (err) {
    logger.error(
      { err },
      "Something went wrong when closing the Pulsar client"
    );
  }
  logger.info("Exit process");
  process.exit(); // eslint-disable-line no-process-exit
};

/**
 * Main function.
 */
/* eslint-disable @typescript-eslint/no-floating-promises */
(async () => {
  /* eslint-enable @typescript-eslint/no-floating-promises */
  try {
    const logger = pino({
      name: "mqtt-pulsar-forwarder",
      timestamp: pino.stdTimeFunctions.isoTime,
    });

    let setHealthOk: (isOk: boolean) => void;
    let pulsarClient: Pulsar.Client;
    let pulsarProducer: Pulsar.Producer;
    let mqttClient: mqtt.AsyncMqttClient;
    let mqttUnsubscribe: () => Promise<void>;

    const exitHandler = (exitError: Error, exitCode: number) => {
      // Exit next.
      /* eslint-disable @typescript-eslint/no-floating-promises */
      exitGracefully(
        logger,
        exitError,
        exitCode,
        setHealthOk,
        mqttClient,
        mqttUnsubscribe,
        pulsarClient,
        pulsarProducer
      );
      /* eslint-enable @typescript-eslint/no-floating-promises */
    };

    try {
      // Handle different kinds of exits.
      process.on("beforeExit", () => exitHandler(new Error("beforeExit"), 1));
      process.on("unhandledRejection", (reason) =>
        exitHandler(transformUnknownToError(reason), 1)
      );
      process.on("uncaughtException", (err) => exitHandler(err, 1));
      process.on("SIGINT", (signal) => exitHandler(new Error(signal), 130));
      process.on("SIGQUIT", (signal) => exitHandler(new Error(signal), 131));
      process.on("SIGTERM", (signal) => exitHandler(new Error(signal), 143));

      logger.info("Read configuration");
      const config = getConfig(logger);
      logger.info("Create health check server");
      setHealthOk = createHealthCheckServer(config.healthCheck);
      logger.info("Create Pulsar client");
      pulsarClient = createPulsarClient(config.pulsar);
      logger.info("Create Pulsar producer");
      pulsarProducer = await createPulsarProducer(pulsarClient, config.pulsar);
      logger.info("Start MQTT subscriber");
      const mqttClientAndUnsubscribe = await createMqttClientAndUnsubscribe(
        logger,
        config.mqtt,
        pulsarProducer
      );
      mqttClient = mqttClientAndUnsubscribe.client;
      mqttUnsubscribe = mqttClientAndUnsubscribe.unsubscribe;
      logger.info("Set health check status as OK");
      setHealthOk(true);
    } catch (err) {
      exitHandler(transformUnknownToError(err), 1);
    }
  } catch (loggerErr) {
    console.error("Failed to start logging:", loggerErr); // eslint-disable-line no-console
    process.exit(1); // eslint-disable-line no-process-exit
  }
})();
