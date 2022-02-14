import { assert } from "console";
import crypto from "crypto";
import fs from "fs";
import mqtt from "mqtt";
import pino from "pino";
import Pulsar from "pulsar-client";

export interface MqttConfig {
  url: string;
  topicFilter: string;
  clientOptions: mqtt.IClientOptions;
  subscribeOptions: mqtt.IClientSubscribeOptions;
}

export interface PulsarConfig {
  authToken: string;
  clientConfig: Pulsar.ClientConfig;
  producerConfig: Pulsar.ProducerConfig;
}

export interface Config {
  mqtt: MqttConfig;
  pulsar: PulsarConfig;
}

const getRequired = (logger: pino.Logger, envVariable: string) => {
  const variable = process.env[envVariable];
  if (typeof variable === "undefined") {
    logger.fatal(`${envVariable} must be defined`);
    process.exit(1);
  }
  return variable;
};

const getOptional = (envVariable: string) => process.env[envVariable];

const getOptionalBooleanWithDefault = (
  logger: pino.Logger,
  envVariable: string,
  defaultValue: boolean
) => {
  let result = defaultValue;
  const str = getOptional(envVariable);
  if (typeof str !== undefined) {
    // Current tsc cannot infer that str cannot be undefined.
    if (!["false", "true"].includes(str as string)) {
      logger.fatal(`${envVariable} must be either "false" or "true"`);
      process.exit(1);
    }
    result = str === "true";
  }
  return result;
};

const getMqttQos = (logger: pino.Logger): mqtt.QoS => {
  const qos = parseInt(process.env.MQTT_QOS || "2", 10);
  if (qos !== 0 && qos !== 1 && qos !== 2) {
    logger.fatal("If defined, MQTT_QOS must be 0, 1 or 2. Default is 2.");
    process.exit(1);
  }
  return qos;
};

const getMqttAuth = (logger: pino.Logger) => {
  let username;
  let password;
  const usernamePath = process.env.MQTT_USERNAME_PATH;
  const passwordPath = process.env.MQTT_PASSWORD_PATH;
  const isUsernamePath = typeof usernamePath !== "undefined";
  const isPasswordPath = typeof passwordPath !== "undefined";
  if (isUsernamePath !== isPasswordPath) {
    logger.fatal(
      "Either both or neither of MQTT_USERNAME_PATH and MQTT_PASSWORD_PATH " +
        "must be defined"
    );
    process.exit(1);
  }
  if (isUsernamePath && isPasswordPath) {
    username = fs.readFileSync(usernamePath, "utf8");
    password = fs.readFileSync(passwordPath, "utf8");
  }
  return { username, password };
};

const createMqttClientId = () => {
  const maxLength = 23;
  const prefix = "pulsar-forwarder-";
  const nSuffix = maxLength - prefix.length;
  const suffix = crypto.randomBytes(16).toString("base64").slice(0, nSuffix);
  const clientId = prefix + suffix;
  assert(clientId.length === maxLength);
  return clientId;
};

const getMqttConfig = (logger: pino.Logger) => {
  const url = getRequired(logger, "MQTT_URL");
  const { username, password } = getMqttAuth(logger);
  const clientId = createMqttClientId();
  const topicFilter = getRequired(logger, "MQTT_TOPIC_FILTER");
  const qos = getMqttQos(logger);
  const clean = getOptionalBooleanWithDefault(logger, "MQTT_CLEAN", false);
  return {
    url,
    topicFilter,
    clientOptions: {
      username,
      password,
      clientId,
      clean,
    },
    subscribeOptions: {
      qos,
    },
  };
};

const getPulsarAuthToken = (logger: pino.Logger) => {
  const authTokenPath = getRequired(logger, "PULSAR_AUTH_TOKEN_PATH");
  return fs.readFileSync(authTokenPath, "utf8");
};

const getPulsarConfig = (logger: pino.Logger) => {
  const authToken = getPulsarAuthToken(logger);
  const serviceUrl = getRequired(logger, "PULSAR_SERVICE_URL");
  const topic = getRequired(logger, "PULSAR_TOPIC");
  const blockIfQueueFull = getOptionalBooleanWithDefault(
    logger,
    "PULSAR_BLOCK_IF_QUEUE_FULL",
    true
  );
  const compressionType = (getOptional("PULSAR_COMPRESSION_TYPE") ||
    "ZSTD") as Pulsar.CompressionType;
  return {
    authToken,
    clientConfig: {
      serviceUrl,
    },
    producerConfig: {
      topic,
      blockIfQueueFull,
      compressionType,
    },
  };
};

export const getConfig = (logger: pino.Logger): Config => ({
  mqtt: getMqttConfig(logger),
  pulsar: getPulsarConfig(logger),
});
