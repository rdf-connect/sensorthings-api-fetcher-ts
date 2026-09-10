import { describe, expect, test } from "vitest";
import { SensorThingsFetcher } from "../src";
import { createWriter, logger } from "@rdfc/js-runner/lib/testUtils";
import type { FullProc } from "@rdfc/js-runner";

const first = "https://iot.hamburg.de/v1.1/Datastreams(26598)";
const second = "https://iot.hamburg.de/v1.1/Datastreams(29728)";

function createProcessor(
    datastream?: string | string[],
    datastreamCollection = "",
) {
    const [writer] = createWriter();
    return new SensorThingsFetcher(
        {
            datastream,
            datastreamCollection,
            writer,
            follow: false,
            maxDatastreams: 0,
        },
        logger,
    ) as FullProc<SensorThingsFetcher>;
}

describe("Datastream configuration", () => {
    test.each([
        { input: first, expected: [first] },
        { input: [first, second], expected: [first, second] },
        { input: `${first}, ${second}`, expected: [first, second] },
        { input: [` ${first}, `, `, ${second} `], expected: [first, second] },
    ])("normalizes $input", async ({ input, expected }) => {
        const processor = createProcessor(input);
        await processor.init();
        expect(processor.datastreams).toEqual(expected);
    });

    test.each([undefined, "", " ,  , ", [], ["", " , "]])(
        "rejects empty datastream configuration %j",
        async (input) => {
            await expect(createProcessor(input).init()).rejects.toThrow(
                "requires either",
            );
        },
    );

    test("rejects datastreams combined with a collection", async () => {
        await expect(
            createProcessor([first], "https://example.com/Datastreams").init(),
        ).rejects.toThrow("requires only a single one");
    });
});
