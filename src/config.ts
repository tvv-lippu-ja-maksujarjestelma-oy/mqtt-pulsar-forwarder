import crypto from "crypto";
import fs from "fs";
import type mqtt from "mqtt";
import type pino from "pino";
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

export interface HealthCheckConfig {
  port: number;
}

export interface Config {
  mqtt: MqttConfig;
  pulsar: PulsarConfig;
  healthCheck: HealthCheckConfig;
}

const getRequired = (envVariable: string) => {
  const variable = process.env[envVariable];
  if (variable === undefined) {
    throw new Error(`${envVariable} must be defined`);
  }
  return variable;
};

const getOptional = (envVariable: string) => process.env[envVariable];

const getOptionalNonNegativeInteger = (
  envVariable: string
): number | undefined => {
  let result;
  const str = getOptional(envVariable);
  if (str !== undefined) {
    const num = parseInt(str, 10);
    if (Number.isNaN(num) || num < 0) {
      throw new Error(
        `If defined, ${envVariable} must be a non-negative integer.`
      );
    }
    result = num;
  }
  return result;
};

const getOptionalBooleanWithDefault = (
  envVariable: string,
  defaultValue: boolean
) => {
  let result = defaultValue;
  const str = getOptional(envVariable);
  if (str !== undefined) {
    if (!["false", "true"].includes(str)) {
      throw new Error(`${envVariable} must be either "false" or "true"`);
    }
    result = str === "true";
  }
  return result;
};

const getMqttQos = (): mqtt.QoS => {
  const qos = parseInt(process.env["MQTT_QOS"] ?? "2", 10);
  if (qos !== 0 && qos !== 1 && qos !== 2) {
    throw new Error("If defined, MQTT_QOS must be 0, 1 or 2. Default is 2.");
  }
  return qos;
};

const getMqttAuth = () => {
  let result;
  const usernamePath = process.env["MQTT_USERNAME_PATH"];
  const passwordPath = process.env["MQTT_PASSWORD_PATH"];
  const isUsernamePath = usernamePath !== undefined;
  const isPasswordPath = passwordPath !== undefined;
  if (isUsernamePath !== isPasswordPath) {
    throw new Error(
      "Either both or neither of MQTT_USERNAME_PATH and MQTT_PASSWORD_PATH " +
        "must be defined"
    );
  }
  if (isUsernamePath && isPasswordPath) {
    result = {
      username: fs.readFileSync(usernamePath, "utf8"),
      password: fs.readFileSync(passwordPath, "utf8"),
    };
  }
  return result;
};

const createMqttClientId = () => {
  const prefix = getRequired("MQTT_CLIENT_ID_PREFIX");
  const suffixLength =
    getOptionalNonNegativeInteger("MQTT_CLIENT_ID_SUFFIX_LENGTH") ?? 0;
  // n random bytes will always result in at least n characters.
  const suffix = crypto
    .randomBytes(suffixLength)
    .toString("base64")
    .slice(0, suffixLength);
  const clientId = prefix + suffix;
  return clientId;
};

const getMqttConfig = (): MqttConfig => {
  const url = getRequired("MQTT_URL");
  const usernameAndPassword = getMqttAuth();
  const clientId = createMqttClientId();
  const topicFilter = getRequired("MQTT_TOPIC_FILTER");
  const qos = getMqttQos();
  const clean = getOptionalBooleanWithDefault("MQTT_CLEAN", false);
  return {
    url,
    topicFilter,
    clientOptions: {
      clientId,
      clean,
      ...usernameAndPassword,
    },
    subscribeOptions: {
      qos,
    },
  };
};

const getPulsarOauth2Config = () => ({
  // pulsar-client requires "type" but that seems unnecessary
  type: "client_credentials",
  issuer_url: getRequired("PULSAR_OAUTH2_ISSUER_URL"),
  private_key: getRequired("PULSAR_OAUTH2_KEY_PATH"),
  audience: getRequired("PULSAR_OAUTH2_AUDIENCE"),
});

const createPulsarLog =
  (logger: pino.Logger) =>
  (
    level: Pulsar.LogLevel,
    file: string,
    line: number,
    message: string
  ): void => {
    switch (level) {
      case Pulsar.LogLevel.DEBUG:
        logger.debug({ file, line }, message);
        break;
      case Pulsar.LogLevel.INFO:
        logger.info({ file, line }, message);
        break;
      case Pulsar.LogLevel.WARN:
        logger.warn({ file, line }, message);
        break;
      case Pulsar.LogLevel.ERROR:
        logger.error({ file, line }, message);
        break;
      default: {
        const exhaustiveCheck: never = level;
        throw new Error(String(exhaustiveCheck));
      }
    }
  };

const getPulsarCompressionType = (): Pulsar.CompressionType => {
  const compressionType = getOptional("PULSAR_COMPRESSION_TYPE") ?? "ZSTD";
  // tsc does not understand:
  // if (!["Zlib", "LZ4", "ZSTD", "SNAPPY"].includes(compressionType)) {
  if (
    compressionType !== "Zlib" &&
    compressionType !== "LZ4" &&
    compressionType !== "ZSTD" &&
    compressionType !== "SNAPPY"
  ) {
    throw new Error(
      "If defined, PULSAR_COMPRESSION_TYPE must be one of 'Zlib', 'LZ4', " +
        "'ZSTD' or 'SNAPPY'. Default is 'ZSTD'."
    );
  }
  return compressionType;
};

const getPulsarConfig = (logger: pino.Logger) => {
  const oauth2Config = getPulsarOauth2Config();
  const serviceUrl = getRequired("PULSAR_SERVICE_URL");
  const tlsValidateHostname = getOptionalBooleanWithDefault(
    "PULSAR_TLS_VALIDATE_HOSTNAME",
    true
  );
  const log = createPulsarLog(logger);
  const topic = getRequired("PULSAR_TOPIC");
  const blockIfQueueFull = getOptionalBooleanWithDefault(
    "PULSAR_BLOCK_IF_QUEUE_FULL",
    true
  );
  const compressionType = getPulsarCompressionType();
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

const getHealthCheckConfig = () => {
  const port = parseInt(getOptional("HEALTH_CHECK_PORT") ?? "8080", 10);
  return { port };
};

export const getConfig = (logger: pino.Logger): Config => ({
  mqtt: getMqttConfig(),
  pulsar: getPulsarConfig(logger),
  healthCheck: getHealthCheckConfig(),
});
