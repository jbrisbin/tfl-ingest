# Ingest London Air Quality Data into Kafka

This application connects to the King's College London Air Quality project website
and downloads historical data for a given range. To change what range is configured,
edit the `src/main/resources/application.properties` file and change the `mars.ingest.from`
and `mars.ingest.to` values.

To run the app, just do:

    > ./gradlew bootRun

You should see messages flowing if consuming from Kafka using the CLI:

    > kafka-console-consumer.sh --zookeeper 127.0.0.1:2181 --topic mars-ingest

The topic name is set in the `application.properties` file as well. Spring Cloud expects
it to be set thus:

    spring.cloud.stream.bindings.output=mars-ingest

Not obvious, I know. :) That's because Spring Cloud Stream has multiple implementations
and the "topic" value (here used with Kafka) needs to be generic across different kinds of
stream bindings like Redis, RabbitMQ, etc...