import mqtt from "mqtt";
import type { ObservationInput } from "./types";

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
    const client = mqtt.connect(brokerUrl);

    client.on("connect", () => {
        const topics = datastreamUris.map((uri) => {
            const topic = datastreamUriToTopic(uri);
            topicToDatastream.set(topic, uri);
            return topic;
        });

        client.subscribe(topics, (err: Error | null) => {
            if (err) {
                onError?.(err);
            }
        });
    });

    client.on("message", async (topic: string, payload: Uint8Array) => {
        try {
            const datastreamUri = topicToDatastream.get(topic);
            if (!datastreamUri) {
                return;
            }

            const parsed = JSON.parse(payload.toString()) as
                | ObservationInput
                | { value: ObservationInput[] };

            const observations: ObservationInput[] = Array.isArray(parsed)
                ? parsed
                : Array.isArray((parsed as { value?: ObservationInput[] }).value)
                  ? (parsed as { value: ObservationInput[] }).value
                  : [parsed as ObservationInput];

            for (const observation of observations) {
                await onObservation({ datastreamUri, observation });
            }
        } catch (err) {
            onError?.(err);
        }
    });

    client.on("error", (err: unknown) => onError?.(err));
}

function datastreamUriToTopic(uri: string): string {
    const match = uri.match(/Datastreams\(([^)]+)\)/i);
    if (!match) {
        throw new Error(`Cannot derive MQTT topic from datastream URI: ${uri}`);
    }

    return `v1.1/Datastreams(${match[1]})/Observations`;
}
