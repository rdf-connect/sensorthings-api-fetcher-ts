import { describe, expect, test } from "vitest";
import { checkProcDefinition, getProc } from "@rdfc/js-runner/lib/testUtils";

import { SensorThingsFetcher, TemplateProcessor } from "../src";

describe("Template processor tests", async () => {
    test("rdfc:SensorThingsFetcher is properly defined", async () => {
        const processorConfig = `
        @prefix rdfc: <https://w3id.org/rdf-connect#>.

        <http://example.com/ns#processor> a rdfc:SensorThingsFetcher;
          rdfc:url <url>;
          rdfc:writer <jw>.
        `;

        const configLocation = process.cwd() + "/processor.ttl";
        await checkProcDefinition(configLocation, "SensorThingsFetcher");

        const processor = await getProc<SensorThingsFetcher>(
            processorConfig,
            "SensorThingsFetcher",
            configLocation,
        );
        await processor.init();

        expect(processor.writer?.constructor.name).toBe("WriterInstance");
        expect(processor.url).toBeTypeOf("string");
    });
});
