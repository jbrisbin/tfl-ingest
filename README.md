# Tranport for London IoT Data Ingest

These example apps pull together a number of technologies to demonstrate ingestion of IoT data from the Transport for London REST-based feeds. It consists of two "halves": a source component (`kafka-source`) that polls the REST API periodically and extracts some data from the returned XML by applying a Groovy-based lambda, then publishes a JSON respresentation of that data to Kafka using Spring Cloud Stream. The other "half" (`riak-sink`) pulls data in from Kafka using Spring Cloud Stream and stores that data in Riak using a random UUID as the key.

### Building and Running

These apps are standard Spring Boot apps, so can be run like any other:

	> cd kafka-source
	> ./gradlew bootRun