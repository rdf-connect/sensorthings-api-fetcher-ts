import mqtt, { IClientOptions } from "mqtt";
import { ObservationInput } from "./types";

type Entity = ObservationInput; // you can define a better interface depending on your STA model

interface SubscribeOptions {
    mqttUrl: string; // e.g. "wss://your-broker/mqtt"
    mqttOpts?: IClientOptions;
    topicPrefix?: string; // e.g. "v1.1" or "v1.0"
    qos?: 0 | 1 | 2;
}

/**
 * Subscribe to updates (new or modified) in an Observation (or entity) collection via MQTT.
 * @param collectionUri e.g. "Datastreams(1)/Observations" or full path including version prefix
 * @param opts connection / topic options
 * @param onMessage callback invoked when new/updated entity arrives
 * @returns handle to unsubscribe / disconnect
 */
export function subscribeToSensorThingsCollection(
    collectionUri: string,
    opts: SubscribeOptions,
    onMessage: (entity: Entity, topic: string) => void,
): { unsubscribe: () => void } {
    const { mqttUrl, mqttOpts, topicPrefix = "", qos = 0 } = opts;

    // Construct topic string. If collectionUri already includes version, skip adding prefix
    let topic = collectionUri;
    if (topicPrefix && !collectionUri.startsWith(topicPrefix + "/")) {
        topic = `${topicPrefix}/${collectionUri}`;
    }

    const client = mqtt.connect(mqttUrl, mqttOpts);

    client.on("connect", () => {
        console.log("[SensorThings Fetcher] connected, subscribing to", topic);
        client.subscribe(topic, { qos }, (err, granted) => {
            if (err) {
                console.error("[SensorThings Fetcher] subscribe error", err);
            } else {
                console.debug(
                    "[SensorThings Fetcher] granted subscriptions",
                    granted,
                );
            }
        });
    });

    client.on("message", (recvTopic, messageBuffer) => {
        try {
            const payloadText = messageBuffer.toString();
            const entity = JSON.parse(payloadText);
            // Optionally you might filter or validate the entity
            console.log(
                `Emitting update for topic ${recvTopic}: ${JSON.stringify(entity, null, 2)}`,
            );
            onMessage(entity, recvTopic);
        } catch (err) {
            console.error(
                "[SensorThings Fetcher] failed to parse message",
                err,
                messageBuffer.toString(),
            );
        }
    });

    client.on("error", (err) => {
        console.error("[SensorThings Fetcher] client error", err);
    });

    function unsubscribe() {
        client.unsubscribe(topic, (err) => {
            if (err)
                console.warn("[SensorThings Fetcher] unsubscribe error", err);
        });
        client.end();
    }

    return {
        unsubscribe,
    };
}
