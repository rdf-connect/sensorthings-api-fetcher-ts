import { Processor, type Writer } from "@rdfc/js-runner";

import type {
    DataStream,
    ExtractedData,
    FeatureOfInterest,
    Location,
    Observation,
    ObservationInput,
    ObservedProperty,
    Sensor,
    Thing,
} from "./types";
import { rateLimitedFetch } from "./ratelimit";
import { subscribeToSensorThingsCollection } from "./mqttSubscribe";
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
    mqttURL: string;
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

    async init(this: TemplateArgs & this): Promise<void> {
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
            for (const datastreamURI of this.datastreams) {
                await processDatastream(
                    datastreamURI,
                    this.writer,
                    this.mqttURL,
                );
            }
            this.writer.close();
        } catch (e) {
            console.error("Error while extracting from SensorThings API: ", e);
            this.writer.close();
        }

        // Function to start the production of data, starting the pipeline.
    }
}

async function processDatastream(
    datastreamURI: string,
    writer: Writer,
    mqttURL?: string,
) {
    // Extract the datastream observations back-to-front
    log(`Processing datastream: ${datastreamURI}`);
    const { datastream, thing, locations, sensor, observedProperty } =
        await extractMetadata(datastreamURI);

    const observationsLink = datastream["Observations@iot.navigationLink"];

    let nextLink: string | undefined = observationsLink;
    let extracted: {
        observation: ObservationInput;
        featureOfInterest: FeatureOfInterest;
    }[] = [];

    // setup the back-to-front filter for the observations
    if (nextLink)
        nextLink = nextLink.includes("?")
            ? nextLink + "&$orderby=resultTime%20asc"
            : nextLink + "?$orderby=resultTime%20asc";

    let foi;

    while (nextLink) {
        const info = await extractObservations(nextLink);
        extracted = info.extracted;
        nextLink = info.nextLink;
        for (const { observation, featureOfInterest } of extracted) {
            foi = featureOfInterest;
            // Setting links
            // datastream.observation = observation["@iot.selfLink"]
            datastream.thing = thing["@iot.selfLink"];
            datastream.observedProperty = observedProperty["@iot.selfLink"];
            datastream.sensor = sensor["@iot.selfLink"];
            thing.locations = [];
            for (const location of locations)
                thing.locations.push(location["@iot.selfLink"]);

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
                datastream: datastream["@iot.selfLink"],
                featureOfInterest: featureOfInterest["@iot.selfLink"],
                phenomenonTime: {
                    hasBeginning: {
                        inXSDDateTimeStamp: startTimeString as string,
                    },
                    hasEnd: { inXSDDateTimeStamp: endTimeString as string },
                    hasXSDDuration: xsdDurationString as string,
                },
            };

            const extractedData: ExtractedData = {
                observation: processedObservation,
                featureOfInterest,
                datastream,
                thing,
                locations,
                sensor,
                observedProperty,
            };

            await writer.string(JSON.stringify(extractedData, null, 2));
        }
    }

    const metadata = {
        featureOfInterest: foi,
        datastream,
        thing,
        locations,
        sensor,
        observedProperty,
    };

    if (mqttURL) {
        console.debug(`Subscribing to the MQTT endpoint at ${mqttURL}`);
        // Setup MQTT subscription to keep up to date with new observations
        let topic = new URL(observationsLink).pathname;
        topic = topic.startsWith("/") ? topic.replace(/^\/+/, "") : topic;
        subscribeToSensorThingsCollection(
            topic,
            { mqttUrl: mqttURL },
            async (message) => {
                const observation: ObservationInput = message;

                const {
                    datastream,
                    thing,
                    locations,
                    sensor,
                    observedProperty,
                } = metadata;
                let featureOfInterest = metadata.featureOfInterest;

                datastream.thing = thing["@iot.selfLink"];
                datastream.observedProperty = observedProperty["@iot.selfLink"];
                datastream.sensor = sensor["@iot.selfLink"];
                thing.locations = [];
                for (const location of locations)
                    thing.locations.push(location["@iot.selfLink"]);

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
                        const startTime = new Date(
                            durationString.split("/")[0],
                        );
                        const endTime = new Date(durationString.split("/")[1]);

                        // compute difference in milliseconds
                        let diffMs = endTime.getTime() - startTime.getTime();
                        if (diffMs < 0) diffMs = 0;

                        // convert milliseconds to ISO 8601 duration components
                        const seconds = Math.floor((diffMs / 1000) % 60);
                        const minutes = Math.floor((diffMs / (1000 * 60)) % 60);
                        const hours = Math.floor(
                            (diffMs / (1000 * 60 * 60)) % 24,
                        );
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

                if (!featureOfInterest) {
                    const newFOI = (await (
                        await rateLimitedFetch(
                            observation["FeatureOfInterest@iot.navigationLink"],
                        )
                    ).json()) as FeatureOfInterest;
                    metadata.featureOfInterest = newFOI;
                    featureOfInterest = newFOI;
                }

                const processedObservation: Observation = {
                    ...observation,
                    datastream: datastream["@iot.selfLink"],
                    featureOfInterest: featureOfInterest["@iot.selfLink"],
                    phenomenonTime: {
                        hasBeginning: {
                            inXSDDateTimeStamp: startTimeString as string,
                        },
                        hasEnd: { inXSDDateTimeStamp: endTimeString as string },
                        hasXSDDuration: xsdDurationString as string,
                    },
                };

                console.debug(
                    `Loaded new entry ${JSON.stringify(processedObservation, null, 2)}`,
                );
                await writer.string(
                    JSON.stringify(processedObservation, null, 2),
                );
            },
        );
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

export function log(message: string) {
    console.debug(`[SensorThings Fetcher]: ${message}`);
}
