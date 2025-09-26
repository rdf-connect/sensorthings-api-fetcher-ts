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
  rdfc:url "https://<endpoint>/Datastreams";
  rdfc:writer <writeChannel> .
```

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