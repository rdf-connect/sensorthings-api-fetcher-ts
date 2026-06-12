import { describe, expect, test } from "vitest";
import { checkProcDefinition, createWriter, logger } from "@rdfc/js-runner/lib/testUtils";
import { FullProc } from "@rdfc/js-runner";

import { SensorThingsFetcher } from "../src";

describe("Template processor tests", async () => {
    test("rdfc:SensorThingsFetcher is properly defined", async () => {
        const configLocation = process.cwd() + "/processor.ttl";
        await checkProcDefinition(configLocation, "SensorThingsFetcher");

        const [writer] = createWriter();
        const processor = <FullProc<SensorThingsFetcher>>new SensorThingsFetcher(
            {
                datastream: "https://iot.hamburg.de/v1.1/Datastreams(29728)",
                follow: false,
                writer,
            },
            logger,
        );

        await processor.init();

        expect(processor.writer?.constructor.name).toBe("WriterInstance");
        expect(processor.datastream).toBeTypeOf("string");
        expect(processor.inputDatastreams).toHaveLength(1);
    });
});
