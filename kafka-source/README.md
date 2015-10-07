# Ingest London Air Quality Data into Kafka

This application connects to the King's College London Air Quality project website
and downloads historical data for a given range. To change what range is configured,
edit the `src/main/resources/application.properties` file and change the `mars.ingest.from`
and `mars.ingest.to` values.

To start the app from Gradle, use the standard Spring Boot convention for starting apps from source:

    $ ./gradlew bootRun

Attach a consumer to Kafka using the CLI if you want to see messages flowing:

    $ kafka-console-consumer.sh --zookeeper 127.0.0.1:2181 --topic mars-ingest

### Start a download

To start a download of data, call the embedded REST API:

    $ curl -v http://localhost:8080/start?species=CO
    > GET /start?species=CO HTTP/1.1
    > Host: localhost:8080
    > User-Agent: curl/7.43.0
    > Accept: */*
    >
    < HTTP/1.1 202 Accepted
    < Server: Apache-Coyote/1.1
    < X-Application-Context: bootstrap:8080
    < Link: rel=info,href=/info/a46046b0-6d37-11e5-7d6b-92ca39710e50
    < Link: rel=stop,href=/stop/a46046b0-6d37-11e5-7d6b-92ca39710e50
    < Content-Length: 0

You can give it a comma-separated string of values of species codes to start downloading,
since you might not want to do all 11 at once (the default if you leave the parameter off).

Valid species codes are:

* TMP - Temperature
* RHUM - Relative Humidity
* SOLR - Solar Radiation
* WDIR - Wind Direction
* WSPD - Wind Speed
* CO - Carbon Monoxide
* NO2 - Nitrogen Dioxide
* O3 - Ozone
* PM10 - PM10 Particulates
* PM25 - PM2.5 Particulates
* SO2 - Sulphur Dioxide

You can also give a wider date range to work with by passing `from` and `to` URL query parameters:

    $ curl -v http://localhost:8080/start?species=CO&from=2015-05-01&to=2015-11-01

The above would start a download of Carbon Monoxide data from May 1st, 2015 to November 1st, 2015.
The ending data is non-inclusive, so would not contain data for November 1st, only October 31st.

### Controlling the download

You can find out if a download is still active (since you might not be able to see the log files
if they're deployed in Mesos) by calling the link denoted by the "info" rel in the Link header
returned from the `/start` request:

    $ curl -v http://localhost:8080/info/a46046b0-6d37-11e5-7d6b-92ca39710e50
    > GET /info/a46046b0-6d37-11e5-7d6b-92ca39710e50 HTTP/1.1
    > Host: localhost:8080
    > User-Agent: curl/7.43.0
    > Accept: */*
    >
    < HTTP/1.1 200 OK
    < Server: Apache-Coyote/1.1
    < X-Application-Context: bootstrap:8080
    < Content-Type: application/json;charset=UTF-8
    < Transfer-Encoding: chunked
    <
    {"active":true}

You can also cancel a download in progress by doing a GET of the "stop" rel:

    $ curl -v http://localhost:8080/stop/a46046b0-6d37-11e5-7d6b-92ca39710e50
    > GET /stop/82874480-6d37-11e5-b2f8-16f1e72f5fb7 HTTP/1.1
    > Host: localhost:8080
    > User-Agent: curl/7.43.0
    > Accept: */*
    >
    < HTTP/1.1 200 OK
    < Server: Apache-Coyote/1.1
    < X-Application-Context: bootstrap:8080
    < Content-Type: application/json;charset=UTF-8
    < Transfer-Encoding: chunked
    <
    {"active":false}

### Configuration

The topic name is set in the `application.properties`. Spring Cloud expects it to be set thus:

    spring.cloud.stream.bindings.output=mars-ingest

Not obvious, I know. :) That's because Spring Cloud Stream has multiple implementations
and the "topic" value (here used with Kafka) needs to be generic across different kinds of
stream bindings like Redis, RabbitMQ, etc...