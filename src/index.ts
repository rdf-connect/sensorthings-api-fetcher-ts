import { Processor, type Writer } from "@rdfc/js-runner";

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
    datastream: string;
    datastreamCollection: string;
    writer: Writer;
    follow: boolean;
    maxDatastreams: number;
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

    datastreams: string[];
    processedObservations: Set<string>;

    async init(this: TemplateArgs & this): Promise<void> {
        this.processedObservations = new Set();

        if (!this.datastream && !this.datastreamCollection) {
            throw new Error(
                "The SensorThings API Fetcher requires either the datastream or datastreamcollection parameter to be provided.",
            );
        }
        if (this.datastream && this.datastreamCollection) {
            throw new Error(
                "The SensorThings API Fetcher requires only a single one of the datastream or datastreamcollection parameters to be provided.",
            );
        }

        if (this.datastreamCollection) {
            log(
                `Sensorthings Fetcher initialized for datastream collection URL: ${this.datastreamCollection}`,
            );
            const datastreams = await extractDatastreams(
                this.datastreamCollection,
            );
            this.datastreams = datastreams;
        } else if (this.datastream) {
            log(
                `Sensorthings Fetcher initialized for datastream URL: ${this.datastream}`,
            );
            this.datastreams = [this.datastream];
        }
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
            const datastreamsToFollow = this.maxDatastreams
                ? this.datastreams
                : this.datastreams.slice(0, this.maxDatastreams);
            for (const datastreamURI of datastreamsToFollow) {
                await this.processDataStream(
                    datastreamURI,
                    this.writer,
                    this.follow,
                );
            }

            if (!this.follow) {
                log(
                    `Finished processing datastreams ${datastreamsToFollow.join(", ")}`,
                );
                await this.writer.close();
            } else {
                await new Promise((resolve, reject) => {});
            }
        } catch (e) {
            console.error("Error while extracting from SensorThings API: ", e);
            await this.writer.close();
        }

        // Function to start the production of data, starting the pipeline.
    }

    async processDataStream(
        datastreamURI: string,
        writer: Writer,
        follow?: boolean,
    ) {
        // Extract the datastream observations back-to-front
        log(`Processing datastream: ${datastreamURI}`);
        const metadata = await extractMetadata(datastreamURI);

        log(`Datastream metadata: ${JSON.stringify(metadata, null, 2)}`);

        const observationsLink =
            metadata.datastream["Observations@iot.navigationLink"];

        let nextLink: string | undefined = observationsLink;

        // setup the back-to-front filter for the observations
        if (nextLink)
            nextLink = nextLink.includes("?")
                ? nextLink + "&$orderby=resultTime%20asc"
                : nextLink + "?$orderby=resultTime%20asc";

        this.processPagedObservations(
            observationsLink,
            metadata,
            writer,
            follow,
        );
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
        follow?: boolean,
    ) {
        let previousLink: string | undefined = undefined;
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
            previousLink = nextLink.slice();
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

        if (follow) {
            setTimeout(
                () =>
                    // We start again at the previously last retrieved offset, since we are going OLD -> NEW (ascending observations)
                    // This way, we prevent new additions messing up while we follow, and we can continue where we left of with the previous offset.
                    // With the way the above loop works, previouslink should never be undefined when arriving here.
                    this.processPagedObservations(
                        previousLink as string,
                        metadataInfo,
                        writer,
                        follow,
                    ),
                30 * 60 * 1000,
            );
        } else {
            // We cannot close the writer in case other datastreams are also being followed
            // And its not that important here to make sure the stream closes?
            // writer.close();
        }
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
    let featureOfInterest = undefined;
    const page = await rateLimitedFetch(url);
    const body = (await page.json()) as {
        value: ObservationInput[];
        ["@iot.nextLink"]?: string;
    };
    const extracted: {
        observation: ObservationInput;
        featureOfInterest: FeatureOfInterest;
    }[] = [];
    for (const observation of body.value) {
        if (featureOfInterest) {
            extracted.push({ observation, featureOfInterest });
        } else {
            featureOfInterest = (await (
                await rateLimitedFetch(
                    observation["FeatureOfInterest@iot.navigationLink"],
                )
            ).json()) as FeatureOfInterest;
            extracted.push({ observation, featureOfInterest });
        }
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
    metadata.datastream.thing = metadata.thing["@iot.selfLink"];
    metadata.datastream.observedProperty =
        metadata.observedProperty["@iot.selfLink"];
    metadata.datastream.sensor = metadata.sensor["@iot.selfLink"];
    metadata.thing.locations = [];
    for (const location of metadata.locations) {
        metadata.thing.locations.push(location["@iot.selfLink"]);
    }

    if (!metadata.featureOfInterest) {
        const newFOI = (await (
            await rateLimitedFetch(
                observationInput["FeatureOfInterest@iot.navigationLink"],
            )
        ).json()) as FeatureOfInterest;
        metadata.featureOfInterest = newFOI;
    }

    return metadata as Metadata;
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
