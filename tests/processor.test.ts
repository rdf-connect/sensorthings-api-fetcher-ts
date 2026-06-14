import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import {
    checkProcDefinition,
    createWriter,
    getProc,
    logger,
} from "@rdfc/js-runner/lib/testUtils";
import { FullProc } from "@rdfc/js-runner";

vi.mock("../src/ratelimit", () => ({
    rateLimitedFetch: vi.fn(),
}));

import { rateLimitedFetch } from "../src/ratelimit";
import { SensorThingsFetcher } from "../src";

const DS_1 = "https://iot.hamburg.de/v1.1/Datastreams(29728)";
const DS_2 = "https://iot.hamburg.de/v1.1/Datastreams(30936)";

const mockedRateLimitedFetch = vi.mocked(rateLimitedFetch);

describe("SensorThingsFetcher processor tests", async () => {
    beforeEach(() => {
        mockedRateLimitedFetch.mockReset();
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    test("rdfc:SensorThingsFetcher is properly defined and initializes multiple datastream metadata", async () => {
        const apiFixtures = buildMetadataFixtures();
        const requestedUrls: string[] = [];

        mockedRateLimitedFetch.mockImplementation(async (url: string) => {
            requestedUrls.push(url);

            const body = apiFixtures.get(url);
            if (!body) {
                throw new Error(`Unexpected URL requested in test fixture: ${url}`);
            }

            return {
                json: async () => structuredClone(body),
            } as unknown as Response;
        });

        const configLocation = process.cwd() + "/processor.ttl";
        await checkProcDefinition(configLocation, "SensorThingsFetcher");

        const [writer] = createWriter();
        const processor = <FullProc<SensorThingsFetcher>>new SensorThingsFetcher(
            {
                datastream: [DS_1, DS_2],
                follow: false,
                writer,
            },
            logger,
        );

        await processor.init();

        expect(processor.writer?.constructor.name).toBe("WriterInstance");
        expect(processor.inputDatastreams).toEqual([DS_1, DS_2]);
        expect(processor.metadataByDatastream.size).toBe(2);
        expect(processor.metadataByDatastream.get(DS_1)?.thing["@iot.selfLink"])
            .toBe("https://iot.hamburg.de/v1.1/Things(10001)");
        expect(processor.metadataByDatastream.get(DS_2)?.sensor["@iot.selfLink"])
            .toBe("https://iot.hamburg.de/v1.1/Sensors(20002)");
        expect(requestedUrls).toContain(DS_1);
        expect(requestedUrls).toContain(DS_2);
        expect(requestedUrls).toContain(
            "https://iot.hamburg.de/v1.1/ObservedProperties(30001)",
        );
        expect(requestedUrls).toContain("https://iot.hamburg.de/v1.1/Sensors(20002)");
        expect(requestedUrls).toContain(
            "https://iot.hamburg.de/v1.1/Things(10002)/Locations",
        );
        expect(requestedUrls.some((url) => url.includes("/Observations")))
            .toBe(false);
    });

    test("repeated rdfc:datastream values are passed as one datastream array", async () => {
        const apiFixtures = buildMetadataFixtures();
        const requestedUrls: string[] = [];

        vi.stubGlobal("fetch", vi.fn(async (url: string) => {
            requestedUrls.push(url);

            const body = apiFixtures.get(url);
            if (!body) {
                throw new Error(`Unexpected URL requested in test fixture: ${url}`);
            }

            return {
                ok: true,
                json: async () => structuredClone(body),
            } as unknown as Response;
        }));

        const processor = await getProc<SensorThingsFetcher>(
            `
@prefix rdfc: <https://w3id.org/rdf-connect#>.

<http://example.com/ns#processor> a rdfc:SensorThingsFetcher;
    rdfc:datastream "${DS_1}", "${DS_2}";
    rdfc:writer <http://example.com/ns#writer>;
    rdfc:follow false.
`,
            "SensorThingsFetcher",
            process.cwd() + "/processor.ttl",
        );

        expect(processor.inputDatastreams).toEqual([DS_1, DS_2]);
        expect(processor.metadataByDatastream.size).toBe(2);
        expect(requestedUrls).toContain(DS_1);
        expect(requestedUrls).toContain(DS_2);
    });
});

function buildMetadataFixtures(): Map<string, unknown> {
    const fixtures = new Map<string, unknown>();

    fixtures.set(DS_1, {
        "@iot.selfLink": DS_1,
        "@iot.id": 29728,
        name: "Datastream 29728",
        description: "Temperature observations",
        observationType: "OM_Measurement",
        unitOfMeasurement: { name: "Celsius", symbol: "C", definition: "" },
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

    return fixtures;
}
