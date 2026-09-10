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
  rdfc:datastream "https://<endpoint>/Datastreams";
  rdfc:writer <writeChannel> .
```

Datastream URLs can be supplied as separate RDF values:

```turtle
rdfc:datastream "https://iot.hamburg.de/v1.1/Datastreams(26598)", "https://iot.hamburg.de/v1.1/Datastreams(29728)";
```

Or as a single comma-separated string, which can also be supplied through an environment variable in your pipeline:

```turtle
rdfc:datastream "https://iot.hamburg.de/v1.1/Datastreams(26598), https://iot.hamburg.de/v1.1/Datastreams(29728)";
```

Both forms can be mixed. Whitespace around URLs is trimmed and empty entries are ignored. Supply at least one non-empty URL, or use `rdfc:datastreamCollection` instead.

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
