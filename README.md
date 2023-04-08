# mongodb-application-sharding

[![Java CI with Maven](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven.yml/badge.svg)](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven.yml)
[![Maven Package](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven-publish.yml/badge.svg)](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven-publish.yml)
[![codecov](https://codecov.io/gh/shashankrnr32/mongodb-application-sharding/branch/main/graph/badge.svg?token=U51FX5G10S)](https://codecov.io/gh/shashankrnr32/mongodb-application-sharding)

[![SonarCloud](https://sonarcloud.io/images/project_badges/sonarcloud-black.svg)](https://sonarcloud.io/summary/new_code?id=shashankrnr32_mongodb-application-sharding)

Mongo DB Application Sharding allows you to shard your Mongo DB cluster from your application using different
strategies. This project is inspired by [Apache's shardingsphere](https://github.com/apache/shardingsphere) which
enables the users to shard the relational databases through the application.

Application Sharding Strategies supported by the library

1. Collection Sharding Strategy
2. Database Sharding Strategy
3. Composite Sharding Strategy

## Get Started

### Spring / Spring Boot Projects

To use this library in your project, just add the package `sharding-spring`
as a dependency to your project.

```xml
<!-- sharding-spring -->
<dependency>
    <groupId>com.alpha.mongodb</groupId>
    <artifactId>sharding-spring</artifactId>
    <version>${mongodb.sharding.version}</version>
</dependency>
```

## Features

1. Supports 3 different sharding strategies for Spring / Spring Boot projects by extending MongoTemplate.
2. Hint Resolution through ThreadLocal and callback based mechanism.
3. Automatic Hint Resolution Callback discovery for Spring Beans using ApplicationContext
4. Custom configuration available for Sharding, validation of shards etc.
5. Tested using Spring's Mongo Template and Repository

## Code Analysis

[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=shashankrnr32_mongodb-application-sharding&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=shashankrnr32_mongodb-application-sharding)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=shashankrnr32_mongodb-application-sharding&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=shashankrnr32_mongodb-application-sharding)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=shashankrnr32_mongodb-application-sharding&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=shashankrnr32_mongodb-application-sharding)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=shashankrnr32_mongodb-application-sharding&metric=bugs)](https://sonarcloud.io/summary/new_code?id=shashankrnr32_mongodb-application-sharding)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=shashankrnr32_mongodb-application-sharding&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=shashankrnr32_mongodb-application-sharding)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=shashankrnr32_mongodb-application-sharding&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=shashankrnr32_mongodb-application-sharding)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=shashankrnr32_mongodb-application-sharding&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=shashankrnr32_mongodb-application-sharding)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=shashankrnr32_mongodb-application-sharding&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=shashankrnr32_mongodb-application-sharding)

## Author

[Shashank Sharma](https://github.com/shashankrnr32)

