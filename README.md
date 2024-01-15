# Demo Projects

[Serverless Application Model (SAM) for Data Professionals](https://jaehyeon.me/blog/2022-07-18-sam-for-data-professionals/)

- [AWS Lambda](https://aws.amazon.com/lambda/) provides serverless computing capabilities and it can be used for performing validation or light processing/transformation of data. Moreover, with its integration with more than 140 AWS services, it facilitates building complex systems employing [event-driven architectures](https://docs.aws.amazon.com/lambda/latest/operatorguide/event-driven-architectures.html). There are many ways to build serverless applications and one of the most efficient ways is using specialised frameworks such as the [AWS Serverless Application Model (SAM)](https://aws.amazon.com/serverless/sam/) and [Serverless Framework](https://www.serverless.com/framework/docs). In this post, I’ll demonstrate how to build a serverless data processing application using SAM.

Kafka Connect for AWS Services Integration - [Aiven OpenSearch Sink Connector](https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka)

- We discuss how to develop a data pipeline from Apache Kafka into [OpenSearch](https://opensearch.org/). In part 1, the pipeline is developed locally using Docker while it is deployed on AWS in the next post.
- [Part 1](https://jaehyeon.me/blog/2023-10-23-kafka-connect-for-aws-part-4/)
- [Part 2](https://jaehyeon.me/blog/2023-10-30-kafka-connect-for-aws-part-5/)

[Setup Local Development Environment for Apache Flink and Spark Using EMR Container Images](https://jaehyeon.me/blog/2023-12-07-flink-spark-local-dev/)

- In this post, we will discuss how to set up a local development environment for Apache Flink and Spark using the EMR container images. For the former, a custom Docker image will be created, which downloads dependent connector Jar files into the Flink library folder, fixes process startup issues, and updates Hadoop configurations for Glue Data Catalog integration. For the latter, instead of creating a custom image, the EMR image is used to launch the Spark container where the required configuration updates are added at runtime via volume-mapping. After illustrating the environment setup, we will discuss a solution where data ingestion/processing is performed in real time using Apache Flink and the processed data is consumed by Apache Spark for analysis.
