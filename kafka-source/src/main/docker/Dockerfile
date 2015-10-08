FROM java:8
VOLUME /tmp
ADD kafka-source-0.1.0.ALPHA1.jar ingest.jar
RUN bash -c 'touch /ingester.jar'
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-jar","/ingest.jar"]