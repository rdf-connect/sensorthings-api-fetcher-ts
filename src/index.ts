import { Processor, type Writer } from "@rdfc/js-runner";
import { subscribeToDatastreamUpdates } from "./mqttSubscribe";

import type {
    DataStream,
    ExtractedData,
    FeatureOfInterest,
    Location,
    Metadata,
    Observation,
    ObservationInput,
    ObservedProperty,
    Sensor,
    Thing,
} from "./types";
import { rateLimitedFetch } from "./ratelimit";

export type {
    DataStream,
    ExtractedData,
    FeatureOfInterest,
    Location,
    Observation,
    ObservedProperty,
    Sensor,
    Thing,
};

type TemplateArgs = {
    datastream?: string;
    datastreams?: string;
    datastreamCollection?: string;
    writer: Writer;
    follow: boolean;
    maxDatastreams?: number;
    mqttBrokerUrl?: string;
};

/**
 * Big todo still: keep the stream up to date with the remote source!
 */

/**
 * The TemplateProcessor class is a very simple processor which simply logs the
 * incoming stream to the RDF-Connect logging system and pipes it directly into
 * the outgoing stream.
 *
 * @param url The URL of the SensorThings API service
 * @param outgoing The data stream into which the incoming stream is written.
 *
 */
export class SensorThingsFetcher extends Processor<TemplateArgs> {
    /**
     * This is the first function that is called (and awaited) when creating a processor.
     * This is the perfect location to start things like database connections.
     */

    inputDatastreams: string[];
    processedObservations: Set<string>;
    metadataByDatastream: Map<
        string,
        {
            datastream: DataStream;
            thing: Thing;
            locations: Location[];
            sensor: Sensor;
            observedProperty: ObservedProperty;
        }
    >;

    async init(this: TemplateArgs & this): Promise<void> {
        this.processedObservations = new Set();
        this.metadataByDatastream = new Map();

        const parsedDatastreams = parseDatastreamList(this.datastreams);
        const hasSingle = !!this.datastream;
        const hasList = parsedDatastreams.length > 0;
        const hasCollection = !!this.datastreamCollection;
        const configuredSources = [hasSingle, hasList, hasCollection].filter(
            Boolean,
        ).length;

        if (configuredSources !== 1) {
            throw new Error(
                "The SensorThings API Fetcher requires exactly one input source: datastream, datastreams, or datastreamCollection.",
            );
        }

        if (hasCollection) {
            log(
                `Sensorthings Fetcher initialized for datastream collection URL: ${this.datastreamCollection}`,
            );
            const datastreams = await extractDatastreams(
                this.datastreamCollection as string,
            );
            this.inputDatastreams = dedupe(datastreams);
        } else if (hasSingle) {
            log(
                `Sensorthings Fetcher initialized for datastream URL: ${this.datastream}`,
            );
            this.inputDatastreams = [this.datastream as string];
        } else {
            this.inputDatastreams = dedupe(parsedDatastreams);
        }

        log(`Initialized with ${this.inputDatastreams.length} datastream(s)`);
    }

    /**
     * Function to start reading channels.
     * This function is called for each processor before `produce` is called.
     * Listen to the incoming stream, log them, and push them to the outgoing stream.
     */
    async transform(this: TemplateArgs & this): Promise<void> {}

    /**
     * Function to start the production of data, starting the pipeline.
     * This function is called after all processors are completely set up.
     */
    async produce(this: TemplateArgs & this): Promise<void> {
        try {
            const datastreamsToProcess =
                this.maxDatastreams && this.maxDatastreams > 0
                    ? this.inputDatastreams.slice(0, this.maxDatastreams)
                    : this.inputDatastreams;

            await runWithConcurrency(
                datastreamsToProcess,
                6,
                async (datastreamURI) => {
                    await this.processDataStream(datastreamURI, this.writer);
                },
            );

            if (!this.follow) {
                log(
                    `Finished processing datastreams ${datastreamsToProcess.join(", ")}`,
                );
                await this.writer.close();
            } else {
                if (!this.mqttBrokerUrl) {
                    throw new Error(
                        "follow=true requires the mqttBrokerUrl parameter.",
                    );
                }

                await subscribeToDatastreamUpdates({
                    brokerUrl: this.mqttBrokerUrl,
                    datastreamUris: datastreamsToProcess,
                    onObservation: async ({
                        datastreamUri,
                        observation,
                    }: {
                        datastreamUri: string;
                        observation: ObservationInput;
                    }) => {
                        if (
                            this.processedObservations.has(
                                observation["@iot.selfLink"],
                            )
                        ) {
                            return;
                        }

                        const metadataInfo =
                            await this.getMetadataInfo(datastreamUri);
                        const metadata = await prepareMetadataObject(
                            {
                                datastream: metadataInfo.datastream,
                                thing: metadataInfo.thing,
                                locations: metadataInfo.locations,
                                sensor: metadataInfo.sensor,
                                observedProperty: metadataInfo.observedProperty,
                            },
                            observation,
                        );

                        const exportObject =
                            await buildExportedObservationObject(
                                observation,
                                metadata,
                            );

                        this.processedObservations.add(
                            observation["@iot.selfLink"],
                        );
                        await this.writer.string(
                            JSON.stringify(exportObject, null, 2),
                        );
                    },
                    onError: (err: unknown) =>
                        console.error("MQTT follow error:", err),
                });

                await new Promise(() => {});
            }
        } catch (e) {
            console.error("Error while extracting from SensorThings API: ", e);
            await this.writer.close();
        }

        // Function to start the production of data, starting the pipeline.
    }

    async getMetadataInfo(datastreamURI: string) {
        const existingMetadata = this.metadataByDatastream.get(datastreamURI);
        if (existingMetadata) {
            return existingMetadata;
        }

        const extractedMetadata = await extractMetadata(datastreamURI);
        this.metadataByDatastream.set(datastreamURI, extractedMetadata);
        return extractedMetadata;
    }

    async processDataStream(datastreamURI: string, writer: Writer) {
        // Extract the datastream observations back-to-front
        log(`Processing datastream: ${datastreamURI}`);
        const metadata = await this.getMetadataInfo(datastreamURI);

        log(`Datastream metadata: ${JSON.stringify(metadata, null, 2)}`);

        const observationsLink =
            metadata.datastream["Observations@iot.navigationLink"];

        let nextLink: string | undefined = observationsLink;

        // setup the back-to-front filter for the observations
        if (nextLink)
            nextLink = nextLink.includes("?")
                ? nextLink + "&$orderby=resultTime%20asc"
                : nextLink + "?$orderby=resultTime%20asc";

        await this.processPagedObservations(nextLink as string, metadata, writer);
    }

    async processPagedObservations(
        startURL: string,
        metadataInfo: {
            datastream: DataStream;
            thing: Thing;
            locations: Location[];
            sensor: Sensor;
            observedProperty: ObservedProperty;
        },
        writer: Writer,
    ) {
        let nextLink: string | undefined = startURL;

        let extracted: {
            observation: ObservationInput;
            featureOfInterest: FeatureOfInterest;
        }[] = [];

        const { datastream, thing, locations, sensor, observedProperty } =
            metadataInfo;

        let metadata:
            | {
                  datastream: DataStream;
                  thing: Thing;
                  featureOfInterest: FeatureOfInterest;
                  locations: Location[];
                  sensor: Sensor;
                  observedProperty: ObservedProperty;
              }
            | undefined;

        // We follow the collection page per page
        while (nextLink) {
            log(`Processing observations in page ${nextLink}`);

            const info = await extractObservations(nextLink);
            extracted = info.extracted;
            nextLink = info.nextLink;

            for (const { observation, featureOfInterest } of extracted) {
                // Skip processed observations
                if (
                    this.processedObservations.has(observation["@iot.selfLink"])
                ) {
                    log(
                        `Skipping observation ${observation["@iot.selfLink"]} due to prior emission`,
                    );
                    continue;
                }

                if (!metadata) {
                    metadata = await prepareMetadataObject(
                        {
                            datastream,
                            thing,
                            featureOfInterest,
                            locations,
                            sensor,
                            observedProperty,
                        },
                        observation,
                    );
                }

                // Build the combined observation + all metadata object for easy RML mapping
                const exportObject = await buildExportedObservationObject(
                    observation,
                    metadata,
                );

                // Save which observations have already been explored
                this.processedObservations.add(observation["@iot.selfLink"]);

                // Emit the resulting observation
                await writer.string(JSON.stringify(exportObject, null, 2));
            }
        }

        // In follow mode, ongoing updates are handled through MQTT subscriptions.
    }
}

async function extractDatastreams(url: string) {
    log(`Extracting datastreams from page ${url}`);
    let datastreams: string[] = [];

    const page = await rateLimitedFetch(url);
    const body = (await page.json()) as {
        value: DataStream[];
        ["@iot.nextLink"]?: string;
    };

    for (const datastream of body.value) {
        datastreams.push(datastream["@iot.selfLink"]);
    }

    const nextLink = body["@iot.nextLink"];
    if (nextLink) {
        datastreams = datastreams.concat(await extractDatastreams(nextLink));
    }
    return datastreams;
}

async function extractMetadata(dataStreamURL: string) {
    log(`Extracting metadata from page ${dataStreamURL}`);

    const dataStreamRes = await rateLimitedFetch(dataStreamURL);
    const dataStreamInfo = (await dataStreamRes.json()) as DataStream;

    const thingRes = await rateLimitedFetch(
        dataStreamInfo["Thing@iot.navigationLink"],
    );
    const thingInfo = (await thingRes.json()) as Thing;

    const sensorRes = await rateLimitedFetch(
        dataStreamInfo["Sensor@iot.navigationLink"],
    );
    const sensorInfo = (await sensorRes.json()) as Sensor;

    const observedPropertyRes = await rateLimitedFetch(
        dataStreamInfo["ObservedProperty@iot.navigationLink"],
    );
    const observedPropertyInfo =
        (await observedPropertyRes.json()) as ObservedProperty;

    const locationsRes = await rateLimitedFetch(
        thingInfo["Locations@iot.navigationLink"],
    );
    const locationsInfo = ((await locationsRes.json()) as { value: Location[] })
        .value as Location[];

    const metadata = {
        datastream: dataStreamInfo,
        thing: thingInfo,
        sensor: sensorInfo,
        observedProperty: observedPropertyInfo,
        locations: locationsInfo,
    };
    return metadata;
}

async function extractObservations(url: string): Promise<{
    extracted: {
        observation: ObservationInput;
        featureOfInterest: FeatureOfInterest;
    }[];
    nextLink: string | undefined;
}> {
    log(`Extracting observations from page ${url}`);
    const page = await rateLimitedFetch(url);
    const body = (await page.json()) as {
        value: ObservationInput[];
        ["@iot.nextLink"]?: string;
    };

    const foiCache = new Map<string, FeatureOfInterest>();

    const extracted: {
        observation: ObservationInput;
        featureOfInterest: FeatureOfInterest;
    }[] = [];

    for (const observation of body.value) {
        const featureOfInterestURL =
            observation["FeatureOfInterest@iot.navigationLink"];
        let featureOfInterest = foiCache.get(featureOfInterestURL);

        if (!featureOfInterest) {
            featureOfInterest = (await (
                await rateLimitedFetch(featureOfInterestURL)
            ).json()) as FeatureOfInterest;
            foiCache.set(featureOfInterestURL, featureOfInterest);
        }

        extracted.push({ observation, featureOfInterest });
    }

    const nextLink = body["@iot.nextLink"];
    return { extracted, nextLink };
}

async function prepareMetadataObject(
    metadata: {
        datastream: DataStream;
        thing: Thing;
        locations: Location[];
        sensor: Sensor;
        observedProperty: ObservedProperty;
        featureOfInterest?: FeatureOfInterest;
    },
    observationInput: ObservationInput,
): Promise<Metadata> {
    const datastream = {
        ...metadata.datastream,
        thing: metadata.thing["@iot.selfLink"],
        observedProperty: metadata.observedProperty["@iot.selfLink"],
        sensor: metadata.sensor["@iot.selfLink"],
    };

    const thing = {
        ...metadata.thing,
        locations: metadata.locations.map((location) => location["@iot.selfLink"]),
    };

    let featureOfInterest = metadata.featureOfInterest;

    if (!featureOfInterest) {
        featureOfInterest = (await (
            await rateLimitedFetch(
                observationInput["FeatureOfInterest@iot.navigationLink"],
            )
        ).json()) as FeatureOfInterest;
    }

    return {
        datastream,
        thing,
        locations: metadata.locations,
        sensor: metadata.sensor,
        observedProperty: metadata.observedProperty,
        featureOfInterest,
    } as Metadata;
}

function parseDatastreamList(datastreams?: string): string[] {
    if (!datastreams?.trim()) {
        return [];
    }

    const trimmed = datastreams.trim();

    if (trimmed.startsWith("[")) {
        try {
            const parsed = JSON.parse(trimmed);
            if (Array.isArray(parsed)) {
                return parsed.filter((value): value is string => {
                    return typeof value === "string";
                });
            }
        } catch {
            return [];
        }
    }

    return trimmed
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean);
}

function dedupe(values: string[]): string[] {
    return [...new Set(values)];
}

async function runWithConcurrency<T>(
    values: T[],
    concurrency: number,
    worker: (value: T) => Promise<void>,
): Promise<void> {
    const queue = [...values];
    const workers = Array.from(
        { length: Math.max(1, concurrency) },
        async () => {
            while (queue.length > 0) {
                const item = queue.shift();
                if (item !== undefined) {
                    await worker(item);
                }
            }
        },
    );

    await Promise.all(workers);
}

async function buildExportedObservationObject(
    observation: ObservationInput,
    metadata: Metadata,
) {
    const durationString = observation.phenomenonTime;
    let startTimeString;
    let endTimeString;
    let xsdDurationString;
    try {
        if (!durationString.includes("/")) {
            // Only a single datetime was provided not a duration
            xsdDurationString = "P0DT0H0M0S";
            endTimeString = durationString;
            startTimeString = durationString;
        } else {
            const startTime = new Date(durationString.split("/")[0]);
            const endTime = new Date(durationString.split("/")[1]);

            // compute difference in milliseconds
            let diffMs = endTime.getTime() - startTime.getTime();
            if (diffMs < 0) diffMs = 0;

            // convert milliseconds to ISO 8601 duration components
            const seconds = Math.floor((diffMs / 1000) % 60);
            const minutes = Math.floor((diffMs / (1000 * 60)) % 60);
            const hours = Math.floor((diffMs / (1000 * 60 * 60)) % 24);
            const days = Math.floor(diffMs / (1000 * 60 * 60 * 24));

            // build xsd:duration string
            xsdDurationString = `P${days}DT${hours}H${minutes}M${seconds}S`;
            endTimeString = endTime.toISOString();
            startTimeString = startTime.toISOString();
        }
    } catch (error) {
        console.error(
            `Could not parse date ${durationString} of ${observation["@iot.selfLink"]}: ${error}.`,
        );
    }

    const processedObservation: Observation = {
        ...observation,
        datastream: metadata.datastream["@iot.selfLink"],
        featureOfInterest: metadata.featureOfInterest["@iot.selfLink"],
        phenomenonTime: {
            hasBeginning: {
                inXSDDateTimeStamp: startTimeString as string,
            },
            hasEnd: { inXSDDateTimeStamp: endTimeString as string },
            hasXSDDuration: xsdDurationString as string,
        },
    };

    return { ...metadata, observation: processedObservation };
}

export function log(message: string) {
    console.debug(`[SensorThings Fetcher]: ${message}`);
}
