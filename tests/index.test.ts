import { createWriter, logger } from "@rdfc/js-runner/lib/testUtils";
import { FullProc } from "@rdfc/js-runner";
import { beforeEach, describe, expect, test, vi } from "vitest";

vi.mock("../src/ratelimit", () => ({
    rateLimitedFetch: vi.fn(),
}));

import { rateLimitedFetch } from "../src/ratelimit";
import { SensorThingsFetcher } from "../src";

type JsonResponse = {
    json: () => Promise<unknown>;
};

const DS_1 = "https://iot.hamburg.de/v1.1/Datastreams(29728)";
const DS_2 = "https://iot.hamburg.de/v1.1/Datastreams(30936)";

const mockedRateLimitedFetch = vi.mocked(rateLimitedFetch);

describe("SensorThingsFetcher functional tests", () => {
    beforeEach(() => {
        mockedRateLimitedFetch.mockReset();
    });

    test("loads two datastreams and emits observation+metadata for each observation", async () => {
        const apiFixtures = buildHamburgFixtures();

        mockedRateLimitedFetch.mockImplementation(async (url: string) => {
            const body = apiFixtures.get(url);
            if (!body) {
                throw new Error(`Unexpected URL requested in test fixture: ${url}`);
            }

            const response: JsonResponse = {
                json: async () => structuredClone(body),
            };

            return response as unknown as Response;
        });

        const [outputWriter, outputReader] = createWriter();

        const proc = <FullProc<SensorThingsFetcher>>new SensorThingsFetcher(
            {
                writer: outputWriter,
                datastreams: JSON.stringify([DS_1, DS_2]),
                follow: false,
            },
            logger,
        );

        await proc.init();

        const readPromise = collectStrings(outputReader);
        await proc.produce();
        const emitted = await readPromise;

        expect(emitted).toHaveLength(3);

        const parsed = emitted.map((entry) => JSON.parse(entry));
        const observationLinks = parsed
            .map((entry) => entry.observation?.["@iot.selfLink"] as string)
            .sort();

        expect(observationLinks).toEqual([
            "https://iot.hamburg.de/v1.1/Observations(90001)",
            "https://iot.hamburg.de/v1.1/Observations(90002)",
            "https://iot.hamburg.de/v1.1/Observations(91001)",
        ]);

        for (const item of parsed) {
            expect(item.datastream?.["@iot.selfLink"]).toBeTypeOf("string");
            expect(item.datastream?.thing).toBe(item.thing?.["@iot.selfLink"]);
            expect(item.datastream?.sensor).toBe(item.sensor?.["@iot.selfLink"]);
            expect(item.datastream?.observedProperty).toBe(
                item.observedProperty?.["@iot.selfLink"],
            );
            expect(Array.isArray(item.locations)).toBe(true);
            expect(item.locations.length).toBeGreaterThan(0);
            expect(item.featureOfInterest?.["@iot.selfLink"]).toBeTypeOf("string");
            expect(item.observation?.datastream).toBe(
                item.datastream?.["@iot.selfLink"],
            );
            expect(item.observation?.featureOfInterest).toBe(
                item.featureOfInterest?.["@iot.selfLink"],
            );
            expect(item.observation?.phenomenonTime?.hasBeginning?.inXSDDateTimeStamp)
                .toBeTypeOf("string");
            expect(item.observation?.phenomenonTime?.hasEnd?.inXSDDateTimeStamp)
                .toBeTypeOf("string");
            expect(item.observation?.phenomenonTime?.hasXSDDuration).toBeTypeOf(
                "string",
            );
        }

        expect(mockedRateLimitedFetch).toHaveBeenCalledWith(DS_1);
        expect(mockedRateLimitedFetch).toHaveBeenCalledWith(DS_2);
        expect(mockedRateLimitedFetch).toHaveBeenCalledWith(
            "https://iot.hamburg.de/v1.1/Datastreams(29728)/Observations?$orderby=resultTime%20asc",
        );
        expect(mockedRateLimitedFetch).toHaveBeenCalledWith(
            "https://iot.hamburg.de/v1.1/Datastreams(30936)/Observations?$orderby=resultTime%20asc",
        );
        expect(mockedRateLimitedFetch).toHaveBeenCalledWith(
            "https://iot.hamburg.de/v1.1/ObservedProperties(30001)",
        );
        expect(mockedRateLimitedFetch).toHaveBeenCalledWith(
            "https://iot.hamburg.de/v1.1/Sensors(20002)",
        );
        expect(mockedRateLimitedFetch).toHaveBeenCalledWith(
            "https://iot.hamburg.de/v1.1/Things(10002)/Locations",
        );
    });
});

async function collectStrings(reader: { strings: () => AsyncIterable<string> }) {
    const values: string[] = [];
    for await (const item of reader.strings()) {
        values.push(item);
    }
    return values;
}

function buildHamburgFixtures(): Map<string, unknown> {
    const fixtures = new Map<string, unknown>();

    fixtures.set(DS_1, {
        "@iot.selfLink": DS_1,
        "@iot.id": 29728,
        name: "Datastream 29728",
        description: "Temperature observations",
        observationType: "OM_Measurement",
        unitOfMeasurement: { name: "Celsius", symbol: "°C", definition: "" },
        observedArea: {},
        phenomenonTime: "2026-06-01T00:00:00Z/2026-06-01T01:00:00Z",
        properties: {},
        resultTime: "2026-06-01T01:00:00Z",
        "Thing@iot.navigationLink": "https://iot.hamburg.de/v1.1/Things(10001)",
        "Sensor@iot.navigationLink": "https://iot.hamburg.de/v1.1/Sensors(20001)",
        "ObservedProperty@iot.navigationLink": "https://iot.hamburg.de/v1.1/ObservedProperties(30001)",
        "Observations@iot.navigationLink": "https://iot.hamburg.de/v1.1/Datastreams(29728)/Observations",
    });

    fixtures.set(DS_2, {
        "@iot.selfLink": DS_2,
        "@iot.id": 30936,
        name: "Datastream 30936",
        description: "Humidity observations",
        observationType: "OM_Measurement",
        unitOfMeasurement: { name: "Percent", symbol: "%", definition: "" },
        observedArea: {},
        phenomenonTime: "2026-06-01T00:00:00Z/2026-06-01T01:00:00Z",
        properties: {},
        resultTime: "2026-06-01T01:00:00Z",
        "Thing@iot.navigationLink": "https://iot.hamburg.de/v1.1/Things(10002)",
        "Sensor@iot.navigationLink": "https://iot.hamburg.de/v1.1/Sensors(20002)",
        "ObservedProperty@iot.navigationLink": "https://iot.hamburg.de/v1.1/ObservedProperties(30002)",
        "Observations@iot.navigationLink": "https://iot.hamburg.de/v1.1/Datastreams(30936)/Observations",
    });

    fixtures.set("https://iot.hamburg.de/v1.1/Things(10001)", {
        "@iot.selfLink": "https://iot.hamburg.de/v1.1/Things(10001)",
        "@iot.id": 10001,
        name: "Thing 10001",
        description: "Station A",
        properties: {},
        "Locations@iot.navigationLink": "https://iot.hamburg.de/v1.1/Things(10001)/Locations",
        "HistoricalLocations@iot.navigationLink": "https://iot.hamburg.de/v1.1/Things(10001)/HistoricalLocations",
    });

    fixtures.set("https://iot.hamburg.de/v1.1/Things(10002)", {
        "@iot.selfLink": "https://iot.hamburg.de/v1.1/Things(10002)",
        "@iot.id": 10002,
        name: "Thing 10002",
        description: "Station B",
        properties: {},
        "Locations@iot.navigationLink": "https://iot.hamburg.de/v1.1/Things(10002)/Locations",
        "HistoricalLocations@iot.navigationLink": "https://iot.hamburg.de/v1.1/Things(10002)/HistoricalLocations",
    });

    fixtures.set("https://iot.hamburg.de/v1.1/Sensors(20001)", {
        "@iot.selfLink": "https://iot.hamburg.de/v1.1/Sensors(20001)",
        "@iot.id": 20001,
        name: "Sensor 20001",
        description: "Temp Sensor",
        encodingType: "application/pdf",
        metadata: "https://example.org/sensors/20001.pdf",
    });

    fixtures.set("https://iot.hamburg.de/v1.1/Sensors(20002)", {
        "@iot.selfLink": "https://iot.hamburg.de/v1.1/Sensors(20002)",
        "@iot.id": 20002,
        name: "Sensor 20002",
        description: "Humidity Sensor",
        encodingType: "application/pdf",
        metadata: "https://example.org/sensors/20002.pdf",
    });

    fixtures.set("https://iot.hamburg.de/v1.1/ObservedProperties(30001)", {
        "@iot.selfLink": "https://iot.hamburg.de/v1.1/ObservedProperties(30001)",
        "@iot.id": 30001,
        name: "air temperature",
        definition: "https://example.org/temperature",
        description: "Air temperature",
    });

    fixtures.set("https://iot.hamburg.de/v1.1/ObservedProperties(30002)", {
        "@iot.selfLink": "https://iot.hamburg.de/v1.1/ObservedProperties(30002)",
        "@iot.id": 30002,
        name: "relative humidity",
        definition: "https://example.org/humidity",
        description: "Relative humidity",
    });

    fixtures.set("https://iot.hamburg.de/v1.1/Things(10001)/Locations", {
        value: [
            {
                "@iot.selfLink": "https://iot.hamburg.de/v1.1/Locations(50001)",
                "@iot.id": 50001,
                name: "Location 50001",
                description: "Station A location",
                encodingType: "application/vnd.geo+json",
                location: { type: "Feature", properties: {}, geometry: {} },
                properties: {},
            },
        ],
    });

    fixtures.set("https://iot.hamburg.de/v1.1/Things(10002)/Locations", {
        value: [
            {
                "@iot.selfLink": "https://iot.hamburg.de/v1.1/Locations(50002)",
                "@iot.id": 50002,
                name: "Location 50002",
                description: "Station B location",
                encodingType: "application/vnd.geo+json",
                location: { type: "Feature", properties: {}, geometry: {} },
                properties: {},
            },
        ],
    });

    fixtures.set(
        "https://iot.hamburg.de/v1.1/Datastreams(29728)/Observations?$orderby=resultTime%20asc",
        {
            value: [
                {
                    "@iot.selfLink": "https://iot.hamburg.de/v1.1/Observations(90001)",
                    "@iot.id": 90001,
                    phenomenonTime: "2026-06-01T00:00:00Z/2026-06-01T00:05:00Z",
                    resultTime: "2026-06-01T00:05:00Z",
                    "FeatureOfInterest@iot.navigationLink": "https://iot.hamburg.de/v1.1/FeaturesOfInterest(40001)",
                    "Datastreamt@iot.navigationLink": DS_1,
                },
            ],
            "@iot.nextLink": "https://iot.hamburg.de/v1.1/Datastreams(29728)/Observations?$skip=1&$orderby=resultTime%20asc",
        },
    );

    fixtures.set(
        "https://iot.hamburg.de/v1.1/Datastreams(29728)/Observations?$skip=1&$orderby=resultTime%20asc",
        {
            value: [
                {
                    "@iot.selfLink": "https://iot.hamburg.de/v1.1/Observations(90002)",
                    "@iot.id": 90002,
                    phenomenonTime: "2026-06-01T00:10:00Z/2026-06-01T00:15:00Z",
                    resultTime: "2026-06-01T00:15:00Z",
                    "FeatureOfInterest@iot.navigationLink": "https://iot.hamburg.de/v1.1/FeaturesOfInterest(40001)",
                    "Datastreamt@iot.navigationLink": DS_1,
                },
            ],
        },
    );

    fixtures.set(
        "https://iot.hamburg.de/v1.1/Datastreams(30936)/Observations?$orderby=resultTime%20asc",
        {
            value: [
                {
                    "@iot.selfLink": "https://iot.hamburg.de/v1.1/Observations(91001)",
                    "@iot.id": 91001,
                    phenomenonTime: "2026-06-01T00:20:00Z/2026-06-01T00:25:00Z",
                    resultTime: "2026-06-01T00:25:00Z",
                    "FeatureOfInterest@iot.navigationLink": "https://iot.hamburg.de/v1.1/FeaturesOfInterest(40002)",
                    "Datastreamt@iot.navigationLink": DS_2,
                },
            ],
        },
    );

    fixtures.set("https://iot.hamburg.de/v1.1/FeaturesOfInterest(40001)", {
        "@iot.selfLink": "https://iot.hamburg.de/v1.1/FeaturesOfInterest(40001)",
        name: "Feature 40001",
        description: "Feature of interest for DS 29728",
        encodingType: "application/vnd.geo+json",
        feature: {},
        properties: {},
    });

    fixtures.set("https://iot.hamburg.de/v1.1/FeaturesOfInterest(40002)", {
        "@iot.selfLink": "https://iot.hamburg.de/v1.1/FeaturesOfInterest(40002)",
        name: "Feature 40002",
        description: "Feature of interest for DS 30936",
        encodingType: "application/vnd.geo+json",
        feature: {},
        properties: {},
    });

    return fixtures;
}
