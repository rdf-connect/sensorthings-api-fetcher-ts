import { Processor, type Reader, type Writer } from "@rdfc/js-runner";

import type { DataStream, ExtractedData, FeatureOfInterest, Location, Observation, ObservedProperty, Sensor, Thing } from "./types";
import { rateLimitedFetch } from "./ratelimit";
// import { subscribeToSensorThingsCollection } from "./mqttSubscribe";
export type { DataStream, ExtractedData, FeatureOfInterest, Location, Observation, ObservedProperty, Sensor, Thing }

type TemplateArgs = {
    datastream: string;
    datastreamCollection: string;
    writer: Writer;
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
            throw new Error('The SensorThings API Fetcher requires either the datastream or datastreamcollection parameter to be provided.')
        }
        if (this.datastream && this.datastreamCollection) {
            throw new Error('The SensorThings API Fetcher requires only a single one of the datastream or datastreamcollection parameters to be provided.')
        }
        
        if (this.datastreamCollection) {
            log(`Sensorthings Fetcher initialized for datastream collection URL: ${this.datastreamCollection}`);
            const datastreams = await extractDatastreams(this.datastreamCollection);
            this.datastreams = datastreams;

        } else if (this.datastream) {
            log(`Sensorthings Fetcher initialized for datastream URL: ${this.datastream}`);
            this.datastreams = [ this.datastream ]
        }
        
        
    }

    /**
     * Function to start reading channels.
     * This function is called for each processor before `produce` is called.
     * Listen to the incoming stream, log them, and push them to the outgoing stream.
     */
    async transform(this: TemplateArgs & this): Promise<void> {
        
    }

    /**
     * Function to start the production of data, starting the pipeline.
     * This function is called after all processors are completely set up.
     */
    async produce(this: TemplateArgs & this): Promise<void> {

        try {
            for (let datastreamURI of this.datastreams) {
                await processDatastream(datastreamURI, this.writer)
                
            }
            this.writer.close()
        } catch (e) {
            console.error("Error while extracting from SensorThings API: ", e)
            this.writer.close()
        }

        // Function to start the production of data, starting the pipeline.
    }
}

async function processDatastream(datastreamURI: string, writer: Writer) {
    
    // Extract the datastream observations back-to-front
    log(`Processing datastream: ${datastreamURI}`)
    const { datastream, thing, locations, sensor, observedProperty } = await extractMetadata(datastreamURI)

    const observationsLink = datastream["Observations@iot.navigationLink"];

    let nextLink: string | undefined = observationsLink;
    let extracted: {observation: Observation, featureOfInterest: FeatureOfInterest}[] = []

    // setup the back-to-front filter for the observations
    if (nextLink) nextLink = nextLink.includes('?') ? nextLink + `&$orderby=resultTime%20asc` : nextLink + `?$orderby=resultTime%20asc`
    
    while (nextLink) {
        let info = await extractObservations(nextLink)
        extracted = info.extracted
        nextLink = info.nextLink
        for (let {observation, featureOfInterest} of extracted) {
            // Setting links
            observation.datastream = datastream["@iot.selfLink"]
            observation.featureOfInterest = featureOfInterest["@iot.selfLink"]
            // datastream.observation = observation["@iot.selfLink"]
            datastream.thing = thing["@iot.selfLink"]
            datastream.observedProperty = observedProperty["@iot.selfLink"]
            datastream.sensor = sensor["@iot.selfLink"]
            thing.locations = []
            for (const location of locations) thing.locations.push(location["@iot.selfLink"])

            const extractedData: ExtractedData = {observation, featureOfInterest, datastream, thing, locations, sensor, observedProperty}

            await writer.string(JSON.stringify(extractedData, null, 2))
        }               
    }
    // Setup MQTT subscription to keep up to date with new observations
    // subscribeToSensorThingsCollection(observationsLink, {mqttUrl: ""}, (messsage => {
    //     console.log('message', message)
    // }) )
}

async function extractDatastreams(url: string) {
    log(`Extracting datastreams from page ${url}`)
    let datastreams: string[] = [];

    const page = await rateLimitedFetch(url);
    const body = await page.json() as { value: DataStream[], ["@iot.nextLink"]?: string }

    for (let datastream of body.value) {
        datastreams.push(datastream["@iot.selfLink"]);
    }

    const nextLink = body["@iot.nextLink"];
    if (nextLink) { 
        datastreams = datastreams.concat(
            await extractDatastreams(nextLink)
        )
    }
    return datastreams;
}



async function extractMetadata(dataStreamURL: string) {
    log(`Extracting metadata from page ${dataStreamURL}`)
    
    const dataStreamRes = await rateLimitedFetch(dataStreamURL)
    const dataStreamInfo = await dataStreamRes.json() as DataStream

    const thingRes = await rateLimitedFetch(dataStreamInfo["Thing@iot.navigationLink"])
    const thingInfo = await thingRes.json() as Thing

    const sensorRes = await rateLimitedFetch(dataStreamInfo["Sensor@iot.navigationLink"])
    const sensorInfo = await sensorRes.json() as Sensor

    const observedPropertyRes = await rateLimitedFetch(dataStreamInfo["ObservedProperty@iot.navigationLink"])
    const observedPropertyInfo = await observedPropertyRes.json() as ObservedProperty

    const locationsRes = await rateLimitedFetch(thingInfo["Locations@iot.navigationLink"])
    const locationsInfo = (await locationsRes.json() as { value: Location[] }).value as Location[]

    const metadata = {
        datastream: dataStreamInfo,
        thing: thingInfo,
        sensor: sensorInfo,
        observedProperty: observedPropertyInfo,
        locations: locationsInfo
    }
    return (metadata)
}

async function extractObservations(url: string): Promise<{ extracted: {observation: Observation, featureOfInterest: FeatureOfInterest}[], nextLink: string | undefined }> {

    log(`Extracting observations from page ${url}`)
    let featureOfInterest = undefined;
    const page = await rateLimitedFetch(url);
    const body = await page.json() as { value: Observation[], ["@iot.nextLink"]?: string }
    const extracted : { observation: Observation, featureOfInterest: FeatureOfInterest}[] = []
    for (let observation of body.value) {
        if (featureOfInterest) {
            extracted.push({ observation, featureOfInterest})
        } else {
            featureOfInterest = await (await rateLimitedFetch(observation["FeatureOfInterest@iot.navigationLink"])).json() as FeatureOfInterest
            extracted.push({ observation, featureOfInterest})
        }
        
    }
    const nextLink = body["@iot.nextLink"];
    return { extracted, nextLink }
}

export function log(message: string) {
    console.debug(`[SensorThings Fetcher]: ${message}`);
}
