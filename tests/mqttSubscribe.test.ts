import { beforeEach, describe, expect, test, vi } from "vitest";

const { connect } = vi.hoisted(() => ({
    connect: vi.fn(),
}));

vi.mock("mqtt", () => ({
    default: {
        connect,
    },
}));

import { subscribeToDatastreamUpdates } from "../src/mqttSubscribe";

const DS_1 = "https://iot.hamburg.de/v1.1/Datastreams(26598)";
const DS_2 = "https://iot.hamburg.de/v1.1/Datastreams(30936)";

describe("subscribeToDatastreamUpdates", () => {
    beforeEach(() => {
        connect.mockReset();
    });

    test("falls back to v1.1/Observations when datastream topics are rejected", async () => {
        const handlers = new Map<string, (...args: unknown[]) => unknown>();
        const subscribe = vi
            .fn()
            .mockImplementationOnce((_topics, callback) => {
                callback({ packet: { granted: [128, 128] } });
            })
            .mockImplementationOnce((_topic, callback) => {
                callback(null);
            });
        const client = {
            on: vi.fn((event: string, handler: (...args: unknown[]) => unknown) => {
                handlers.set(event, handler);
                return client;
            }),
            subscribe,
        };

        connect.mockReturnValue(client);
        const onObservation = vi.fn();
        const onError = vi.fn();

        await subscribeToDatastreamUpdates({
            brokerUrl: "mqtt://iot.hamburg.de",
            datastreamUris: [DS_1, DS_2],
            onObservation,
            onError,
        });

        handlers.get("connect")?.();

        expect(subscribe).toHaveBeenNthCalledWith(
            1,
            [
                "v1.1/Datastreams(26598)/Observations",
                "v1.1/Datastreams(30936)/Observations",
            ],
            expect.any(Function),
        );
        expect(subscribe).toHaveBeenNthCalledWith(
            2,
            "v1.1/Observations",
            expect.any(Function),
        );
        expect(onError).not.toHaveBeenCalled();

        await handlers.get("message")?.(
            "v1.1/Observations",
            Buffer.from(
                JSON.stringify({
                    "@iot.selfLink": "https://iot.hamburg.de/v1.1/Observations(1)",
                    "@iot.id": 1,
                    phenomenonTime: "2026-06-14T00:00:00Z",
                    resultTime: "2026-06-14T00:00:00Z",
                    "Datastream@iot.navigationLink": DS_1,
                    "FeatureOfInterest@iot.navigationLink": "https://iot.hamburg.de/v1.1/FeaturesOfInterest(1)",
                }),
            ),
        );

        await handlers.get("message")?.(
            "v1.1/Observations",
            Buffer.from(
                JSON.stringify({
                    "@iot.selfLink": "https://iot.hamburg.de/v1.1/Observations(2)",
                    "@iot.id": 2,
                    phenomenonTime: "2026-06-14T00:00:00Z",
                    resultTime: "2026-06-14T00:00:00Z",
                    "Datastream@iot.navigationLink": "https://iot.hamburg.de/v1.1/Datastreams(99999)",
                    "FeatureOfInterest@iot.navigationLink": "https://iot.hamburg.de/v1.1/FeaturesOfInterest(2)",
                }),
            ),
        );

        expect(onObservation).toHaveBeenCalledTimes(1);
        expect(onObservation).toHaveBeenCalledWith(
            expect.objectContaining({
                datastreamUri: DS_1,
                observation: expect.objectContaining({ "@iot.id": 1 }),
            }),
        );
    });
});