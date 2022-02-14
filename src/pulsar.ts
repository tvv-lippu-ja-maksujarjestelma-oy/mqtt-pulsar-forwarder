import pino from "pino";
import Pulsar from "pulsar-client";
import { PulsarConfig } from "./config";

const createPulsarProducer = async (
  logger: pino.Logger,
  { oauth2Config, clientConfig, producerConfig }: PulsarConfig
) => {
  logger.info("Connect to Pulsar");
  const authentication = new Pulsar.AuthenticationOauth2(oauth2Config);
  const client = new Pulsar.Client({ ...clientConfig, authentication });
  return client.createProducer(producerConfig);
};

export default createPulsarProducer;
