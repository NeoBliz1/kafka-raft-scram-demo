# Kafka Raft SCRAM Demo

This service demonstrates the use of Kafka with KRaft and SCRAM authentication for secure and reliable message delivery.
It's designed to showcase data ingestion from various sources, sending it to a Kafka topic for further processing.

This repository also includes a comprehensive Docker Compose setup for Kafka, enabling robust testing of various data
delivery and authentication scenarios. The included tests cover a range of conditions, from guaranteed message delivery
to handling authentication failures, ensuring the system's resilience and reliability.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing
purposes.

### Prerequisites

* Java 21
* Maven
* Docker

### Installing

1. Clone the repository
2. Build the project with `mvn clean install`
3. Run the application with `java -jar target/eco-ingestion-service-0.0.1-SNAPSHOT.jar`

## Built With

* [Spring Boot](https://spring.io/projects/spring-boot) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [Kafka](https://kafka.apache.org/) - Message Broker
* [Protocol Buffers](https://developers.google.com/protocol-buffers) - Data Serialization
* [Docker Compose](https://docs.docker.com/compose/) - For managing multi-container Docker applications
