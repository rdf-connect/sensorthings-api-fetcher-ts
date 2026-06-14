# sensorthings-api-fetcher-ts

Fetcher component to retrieve the observations from the datastreams found at the provided URL using the SensorThings API

## Usage

To use the processor in your RDF-Connect pipeline, you need to have a pipeline configuration that includes the [rdfc:NodeRunner](https://github.com/rdf-connect/js-runner) (check out their documentation to find out how to install and configure it).

Next, you can add the JS/TS TemplateProcessor to your pipeline configuration as follows:

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.

# Import the processor
<> owl:imports <./node_modules/@rdfc/sensorthings-api-fetcher-ts/processor.ttl>.

### Define the pipeline
<> a rdfc:Pipeline;
  rdfc:consistsOf [
    rdfc:instantiates rdfc:NodeRunner;
    rdfc:processor <fetcher>; 
  ].

# fetcher to get SensorThings API data
<fetcher> a rdfc:SensorThingsFetcher;
  rdfc:datastream "https://<endpoint>/Datastreams(<id>)";
  rdfc:writer <writeChannel> .
```

To fetch multiple datastreams, repeat `rdfc:datastream` values. RDF-Connect
maps these repeated values to the processor's `datastream` argument.

```turtle
<fetcher> a rdfc:SensorThingsFetcher;
  rdfc:datastream "https://iot.hamburg.de/v1.1/Datastreams(29728)",
                 "https://iot.hamburg.de/v1.1/Datastreams(30936)";
  rdfc:follow true;
  rdfc:mqttBrokerUrl "mqtt://example-broker";
  rdfc:writer <writeChannel> .
```

Exactly one input source should be configured: repeated `rdfc:datastream` or a single `rdfc:datastreamCollection`.
Here, `rdfc:maxDatastreams` limits the amount of datastreams discovered through `rdfc:datastreamCollection`.

## Data output

The output stream of the component provides the observation and all connected metadata as a JSON object using the following structure: 

```javascript
{
  observation: Observation,
  datastream: DataStream,
  thing: Thing,
  observedProperty: ObservedProperty,
  featureOfInterest: FeatureOfInterest,
  locations: Location[],
  sensor: Sensor,
}
```