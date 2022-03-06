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

export interface PulsarOauth2Config {
  // pulsar-client requires "type" but that seems unnecessary
  type: string;
  issuer_url: string;
  client_id?: string;
  client_secret?: string;
  private_key?: string;
  audience?: string;
  scope?: string;
}

export interface PulsarConfig {
  oauth2Config: PulsarOauth2Config;
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
  assert(prefix.length <= maxLength);
  const nSuffix = maxLength - prefix.length;
  const suffix = crypto
    .randomBytes(maxLength)
    .toString("base64")
    .slice(0, nSuffix);
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

const getPulsarOauth2Config = (logger: pino.Logger) => ({
  // pulsar-client requires "type" but that seems unnecessary
  type: "client_credentials",
  issuer_url: getRequired(logger, "PULSAR_OAUTH2_ISSUER_URL"),
  private_key: getRequired(logger, "PULSAR_OAUTH2_KEY_PATH"),
  audience: getRequired(logger, "PULSAR_OAUTH2_AUDIENCE"),
});

const matchPulsarLogLevelToPinoLevel = (
  logger: pino.Logger,
  logLevel: Pulsar.LogLevel
): pino.LogFn => {
  switch (logLevel) {
    case Pulsar.LogLevel.DEBUG:
      return logger.debug;
    case Pulsar.LogLevel.INFO:
      return logger.info;
    case Pulsar.LogLevel.WARN:
      return logger.warn;
    case Pulsar.LogLevel.ERROR:
      return logger.error;
  }
};

const createPulsarLog =
  (logger: pino.Logger) =>
  (
    level: Pulsar.LogLevel,
    file: string,
    line: number,
    message: string
  ): void => {
    const logFunc = matchPulsarLogLevelToPinoLevel(logger, level);
    logFunc({ file, line }, message);
  };

const getPulsarConfig = (logger: pino.Logger) => {
  const oauth2Config = getPulsarOauth2Config(logger);
  const serviceUrl = getRequired(logger, "PULSAR_SERVICE_URL");
  const tlsValidateHostname = getOptionalBooleanWithDefault(
    logger,
    "PULSAR_TLS_VALIDATE_HOSTNAME",
    true
  );
  const log = createPulsarLog(logger);
  const topic = getRequired(logger, "PULSAR_TOPIC");
  const blockIfQueueFull = getOptionalBooleanWithDefault(
    logger,
    "PULSAR_BLOCK_IF_QUEUE_FULL",
    true
  );
  const compressionType = (getOptional("PULSAR_COMPRESSION_TYPE") ||
    "ZSTD") as Pulsar.CompressionType;
  return {
    oauth2Config,
    clientConfig: {
      serviceUrl,
      tlsValidateHostname,
      log,
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
