# Ingest London Air Quality Data into Kafka

This application connects to the King's College London Air Quality project website and downloads historical data for a given range. To change what range is configured, edit the `src/main/resources/application.properties` file and change the `mars.ingest.from` and `mars.ingest.to` values.

To run the app, just do:

    > ./gradlew bootRun

You should see messages flowing if consuming from Kafka using the CLI:

    > kafka-console-consumer.sh --zookeeper 127.0.0.1:2181 --topic mars-ingest