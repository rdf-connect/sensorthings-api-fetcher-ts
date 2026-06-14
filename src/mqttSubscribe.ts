import mqtt from "mqtt";
import { rateLimitedFetch } from "./ratelimit";
import type { DataStream, ObservationInput } from "./types";

type ObservationUpdate = ObservationInput & {
    "Datastream@iot.navigationLink"?: string;
    "Datastreamt@iot.navigationLink"?: string;
};

export async function subscribeToDatastreamUpdates(args: {
    brokerUrl: string;
    datastreamUris: string[];
    onObservation: (input: {
        datastreamUri: string;
        observation: ObservationInput;
    }) => Promise<void>;
    onError?: (err: unknown) => void;
}): Promise<void> {
    const { brokerUrl, datastreamUris, onObservation, onError } = args;

    const topicToDatastream = new Map<string, string>();
    const datastreamByKey = new Map(
        datastreamUris.map((uri) => [datastreamKey(uri), uri]),
    );
    const client = mqtt.connect(brokerUrl, {
        reconnectPeriod: 2_000,
        connectTimeout: 30_000,
        resubscribe: true,
        keepalive: 60,
    });

    client.on("connect", () => {
        const topics = datastreamUris.map((uri) => {
            const topic = datastreamUriToTopic(uri);
            topicToDatastream.set(topic, uri);
            return topic;
        });

        client.subscribe(topics, (err: Error | null) => {
            if (isSubscriptionRejected(err)) {
                subscribeToObservationCollection();
                return;
            }

            if (err) {
                onError?.(err);
            }
        });
    });

    function subscribeToObservationCollection() {
        topicToDatastream.clear();
        const observationsTopic = "v1.1/Observations";
        client.subscribe(observationsTopic, (err: Error | null) => {
            if (err) {
                onError?.(err);
            }
        });
    }

    client.on("message", async (topic: string, payload: Uint8Array) => {
        try {
            const parsed = JSON.parse(payload.toString()) as
                | ObservationUpdate
                | ObservationUpdate[]
                | { value: ObservationUpdate[] };
            const observations = normalizeObservationPayload(parsed);

            for (const observation of observations) {
                const datastreamUri = await resolveDatastreamUri(
                    topic,
                    observation,
                    topicToDatastream,
                    datastreamByKey,
                );

                if (!datastreamUri) {
                    continue;
                }

                await onObservation({ datastreamUri, observation });
            }
        } catch (err) {
            onError?.(err);
        }
    });

    client.on("error", (err: unknown) => onError?.(err));
    client.on("reconnect", () =>
        onError?.(new Error("MQTT connection lost, reconnecting...")),
    );
}

function normalizeObservationPayload(
    parsed: ObservationUpdate | ObservationUpdate[] | { value: ObservationUpdate[] },
) {
    if (Array.isArray(parsed)) {
        return parsed;
    }

    if (Array.isArray((parsed as { value?: ObservationUpdate[] }).value)) {
        return (parsed as { value: ObservationUpdate[] }).value;
    }

    return [parsed as ObservationUpdate];
}

async function resolveDatastreamUri(
    topic: string,
    observation: ObservationUpdate,
    topicToDatastream: Map<string, string>,
    datastreamByKey: Map<string, string>,
) {
    const topicDatastream = topicToDatastream.get(topic);
    if (topicDatastream) {
        return topicDatastream;
    }

    const linkedDatastream =
        observation["Datastream@iot.navigationLink"] ??
        observation["Datastreamt@iot.navigationLink"];
    const datastreamUri = linkedDatastream ?? (await fetchObservationDatastream(observation));
    if (!datastreamUri) {
        return undefined;
    }

    return datastreamByKey.get(datastreamKey(datastreamUri));
}

async function fetchObservationDatastream(observation: ObservationUpdate) {
    const observationLink = observation["@iot.selfLink"];
    if (!observationLink) {
        return undefined;
    }

    const response = await rateLimitedFetch(`${observationLink}/Datastream`);
    const datastream = (await response.json()) as DataStream;
    return datastream["@iot.selfLink"];
}

function isSubscriptionRejected(err: Error | null) {
    const granted = (err as (Error & { packet?: { granted?: number[] } }) | null)
        ?.packet?.granted;

    return Array.isArray(granted) && granted.every((qos) => qos === 128);
}

function datastreamUriToTopic(uri: string): string {
    const match = uri.match(/Datastreams\(([^)]+)\)/i);
    if (!match) {
        throw new Error(`Cannot derive MQTT topic from datastream URI: ${uri}`);
    }

    return `v1.1/Datastreams(${match[1]})/Observations`;
}

function datastreamKey(uri: string): string {
    const match = uri.match(/Datastreams\(([^)]+)\)/i);
    return match ? `Datastreams(${match[1]})` : uri;
}
