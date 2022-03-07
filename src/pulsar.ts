import type pino from "pino";
import Pulsar from "pulsar-client";
import type { PulsarConfig } from "./config";

const createPulsarClientAndProducer = async (
  logger: pino.Logger,
  { oauth2Config, clientConfig, producerConfig }: PulsarConfig
) => {
  logger.info("Connect to Pulsar");
  const authentication = new Pulsar.AuthenticationOauth2(oauth2Config);
  const client = new Pulsar.Client({ ...clientConfig, authentication });
  const producer = await client.createProducer(producerConfig);
  return { client, producer };
};

export default createPulsarClientAndProducer;
