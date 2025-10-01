export type ExtractedData = {
    observation: Observation;
    datastream: DataStream;
    thing: Thing;
    observedProperty: ObservedProperty;
    featureOfInterest: FeatureOfInterest;
    locations: Location[];
    sensor: Sensor;
};

export type ObservationInput = {
    "@iot.selfLink": string;
    "@iot.id": number;
    phenomenonTime: string;
    resultTime: string;
    "FeatureOfInterest@iot.navigationLink": string;
    "Datastreamt@iot.navigationLink": string;
};

export type Observation = {
    "@iot.selfLink": string;
    phenomenonTime: XSDDuration;
    resultTime: string;
    featureOfInterest: string;
    datastream: string;
};

export type DataStream = {
    "@iot.selfLink": string;
    "@iot.id": number;
    name: string;
    description: string;
    observationType: string;
    unitOfMeasurement: object; // todo
    observedArea: object; // todo
    phenomenonTime: string;
    properties: object;
    resultTime: string;
    "Thing@iot.navigationLink": string;
    "Sensor@iot.navigationLink": string;
    "ObservedProperty@iot.navigationLink": string;
    "Observations@iot.navigationLink": string;
    thing: string;
    sensor: string;
    observedProperty: string;
    observation: string;
};

export type Thing = {
    "@iot.selfLink": string;
    "@iot.id": number;
    name: string;
    description: string;
    properties: object;
    "Locations@iot.navigationLink": string;
    "HistoricalLocations@iot.navigationLink": string;
    locations: string[];
};

export type ObservedProperty = {
    "@iot.selfLink": string;
    "@iot.id": number;
    name: string;
    definition: string;
    description: string;
};

export type Sensor = {
    "@iot.selfLink": string;
    "@iot.id": number;
    name: string;
    description: string;
    encodingType: string;
    metadata: string;
};

export type Location = {
    "@iot.selfLink": string;
    "@iot.id": number;
    name: string;
    description: string;
    encodingType: string;
    location: {
        type: string;
        properties: object;
        geometry: object;
    };
    properties: object;
};

export type FeatureOfInterest = {
    "@iot.selfLink": string;
    name: string;
    description: string;
    encodingType: string;
    feature: object;
    properties: object;
};

export type XSDDuration = {
    hasBeginning: { inXSDDateTimeStamp: string };
    hasEnd: { inXSDDateTimeStamp: string };
    hasXSDDuration: string;
};
