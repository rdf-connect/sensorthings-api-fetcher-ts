import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { checkProcDefinition, getProc } from "@rdfc/js-runner/lib/testUtils";
import type { ExtractedData, SensorThingsFetcher } from "../src";

const collection = "https://example.com/v1.1/Datastreams";
const urls = [26598, 29728].map((id) => `${collection}(${id})`);
const configLocation = process.cwd() + "/processor.ttl";

// Small, deterministic SensorThings responses; no live endpoint is contacted.
function responses() {
    const pages = new Map<string, object>([
        [
            collection,
            {
                value: [{ "@iot.selfLink": urls[0] }],
                "@iot.nextLink": `${collection}?page=2`,
            },
        ],
        [`${collection}?page=2`, { value: [{ "@iot.selfLink": urls[1] }] }],
    ]);
    for (const url of urls) {
        pages.set(url, {
            "@iot.selfLink": url,
            "Thing@iot.navigationLink": `${url}/Thing`,
            "Sensor@iot.navigationLink": `${url}/Sensor`,
            "ObservedProperty@iot.navigationLink": `${url}/ObservedProperty`,
            "Observations@iot.navigationLink": `${url}/Observations`,
        });
        pages.set(`${url}/Thing`, {
            "@iot.selfLink": `${url}/Thing`,
            "Locations@iot.navigationLink": `${url}/Locations`,
        });
        pages.set(`${url}/Locations`, {
            value: [{ "@iot.selfLink": `${url}/Location` }],
        });
        for (const resource of [
            "Sensor",
            "ObservedProperty",
            "FeatureOfInterest",
        ]) {
            pages.set(`${url}/${resource}`, {
                "@iot.selfLink": `${url}/${resource}`,
            });
        }
        const observation = (id: number) => ({
            "@iot.selfLink": `${url}/Observations(${id})`,
            phenomenonTime: "2026-01-01T00:00:00Z",
            resultTime: "2026-01-01T00:00:00Z",
            result: id,
            "FeatureOfInterest@iot.navigationLink": `${url}/FeatureOfInterest`,
        });
        pages.set(`${url}/Observations`, {
            value: [observation(1)],
            "@iot.nextLink": `${url}/Observations?page=2`,
        });
        pages.set(`${url}/Observations?page=2`, { value: [observation(2)] });
    }
    return pages;
}

describe("SensorThingsFetcher RDF configuration and lifecycle", () => {
    beforeEach(() => {
        vi.useFakeTimers();
        const pages = responses();
        vi.stubGlobal(
            "fetch",
            vi.fn(async (url: string) => {
                const body = pages.get(url);
                if (!body) throw new Error(`Unexpected fetch: ${url}`);
                return new Response(JSON.stringify(body));
            }),
        );
    });
    afterEach(() => {
        vi.useRealTimers();
        vi.unstubAllGlobals();
        vi.restoreAllMocks();
    });

    test("processor implementation is properly defined", async () => {
        await checkProcDefinition(configLocation, "SensorThingsFetcher");
    });

    test.each<{ name: string; property: string; limit?: number }>([
        {
            name: "multiple RDF values",
            property: `rdfc:datastream "${urls[0]}", "${urls[1]}"`,
        },
        {
            name: "comma-separated string",
            property: `rdfc:datastream "${urls.join(", ")}"`,
        },
        {
            name: "mixed values",
            property: `rdfc:datastream "${urls[0]}, ", "${urls[1]}"`,
        },
        {
            name: "datastream collection",
            property: `rdfc:datastreamCollection "${collection}"`,
        },
        {
            name: "datastream limit",
            property: `rdfc:datastreamCollection "${collection}"`,
            limit: 1,
        },
    ])(
        "starts and fetches observations using $name",
        async ({ property, limit }) => {
            const expectedUrls =
                limit === undefined ? urls : urls.slice(0, limit);
            // getProc loads the actual implementation from processor.ttl and calls init.
            const starting = getProc<SensorThingsFetcher>(
                `@prefix rdfc: <https://w3id.org/rdf-connect#>.
             <http://example.com/ns#processor> a rdfc:SensorThingsFetcher;
                 ${property};
                 ${limit === undefined ? "" : `rdfc:maxDatastreams ${limit};`}
                 rdfc:follow false;
                 rdfc:writer <http://example.com/writer>.`,
                "SensorThingsFetcher",
                configLocation,
            );
            // Startup includes asynchronous module loading before collection fetching.
            const processor = await (async () => {
                let ready = false;
                starting.then(
                    () => {
                        ready = true;
                    },
                    () => {
                        ready = true;
                    },
                );
                await vi.waitFor(async () => {
                    await vi.runAllTimersAsync();
                    expect(ready).toBe(true);
                });
                return starting;
            })();
            expect(processor.writer.constructor.name).toBe("WriterInstance");
            expect(processor.datastreams).toEqual(urls);
            expect(processor.processedObservations.size).toBe(0);

            const output: ExtractedData[] = [];
            let closed = false;
            vi.spyOn(processor.writer, "string").mockImplementation(
                async (value) => {
                    expect(
                        closed,
                        "writer must stay open until observations are emitted",
                    ).toBe(false);
                    output.push(JSON.parse(value));
                },
            );
            const close = vi
                .spyOn(processor.writer, "close")
                .mockImplementation(async () => {
                    closed = true;
                });
            await processor.transform();
            const producing = processor.produce();
            await vi.runAllTimersAsync();
            await producing;

            expect(
                output.map((entry) => entry.observation["@iot.selfLink"]),
            ).toEqual(
                expectedUrls.flatMap((url) =>
                    [1, 2].map((id) => `${url}/Observations(${id})`),
                ),
            );
            for (const [index, entry] of output.entries()) {
                const url = expectedUrls[Math.floor(index / 2)];
                expect(entry.datastream["@iot.selfLink"]).toBe(url);
                expect(entry.observation.datastream).toBe(url);
                expect(entry.observation.featureOfInterest).toBe(
                    `${url}/FeatureOfInterest`,
                );
                expect(entry.thing["@iot.selfLink"]).toBe(`${url}/Thing`);
                expect(entry.sensor["@iot.selfLink"]).toBe(`${url}/Sensor`);
                expect(entry.observedProperty["@iot.selfLink"]).toBe(
                    `${url}/ObservedProperty`,
                );
                expect(entry.locations).toEqual([
                    { "@iot.selfLink": `${url}/Location` },
                ]);
            }
            const expectedRequests = [...responses().keys()].filter(
                (url) =>
                    expectedUrls.some((stream) => url.startsWith(stream)) ||
                    (property.includes("datastreamCollection") &&
                        [collection, `${collection}?page=2`].includes(url)),
            );
            expect(
                new Set(vi.mocked(fetch).mock.calls.map(([url]) => url)),
            ).toEqual(new Set(expectedRequests));
            expect(close).toHaveBeenCalledTimes(1);
            expect(processor.processedObservations.size).toBe(
                expectedUrls.length * 2,
            );
        },
    );
});
