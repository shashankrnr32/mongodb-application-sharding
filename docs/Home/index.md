# Mongo DB Application Sharding

[![Java CI with Maven](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven.yml/badge.svg)](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven.yml)
[![Maven Package](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven-publish.yml/badge.svg)](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven-publish.yml)
[![codecov](https://codecov.io/gh/shashankrnr32/mongodb-application-sharding/branch/main/graph/badge.svg?token=U51FX5G10S)](https://codecov.io/gh/shashankrnr32/mongodb-application-sharding)

[![SonarCloud](https://sonarcloud.io/images/project_badges/sonarcloud-black.svg)](https://sonarcloud.io/summary/new_code?id=shashankrnr32_mongodb-application-sharding)

Mongo DB Application Sharding enables you to shard your Mongo DB cluster from your application using different
strategies. This project is inspired by [Apache's shardingsphere](https://github.com/apache/shardingsphere) which
enables the users to shard the relational databases through the application.

Application Sharding Strategies supported by the library

1. [Collection](#collection-sharding-strategy)
2. [Database](#database-sharding-strategy)
3. [Composite](#composite-sharding-strategy)

## Collection Sharding Strategy

Sharding strategy where data is divided into multiple collections in a single database identified by a shardHint
(usually a suffix to the collection name). Collection Sharding strategy enables you to divide your whole data into
multiple collections based on a Shard Hint usually stored as a field inside the collection.

```shell
ds0> show collections
TEST_0
TEST_1
TEST_2
```

The writes routes the operations to the relevant collection based on the shard hint set in the context. The reads
resolve the route based on the shard hint set in the context and routes the query to the right collection.

## Database Sharding Strategy

Sharding strategy where data is divided into multiple databases with the collection name being same across multiple
databases.

```shell
> show databases
DatabaseShardedDS0          72.00 KiB
DatabaseShardedDS1          40.00 KiB
DatabaseShardedDS2          72.00 KiB

> use DatabaseShardedDS0
'switched to db DatabaseShardedDS0'
DatabaseShardedDS0> show collections
TEST

> use DatabaseShardedDS1
'switched to db DatabaseShardedDS1'
DatabaseShardedDS1> show collections
TEST

> use DatabaseShardedDS2
'switched to db DatabaseShardedDS2'
DatabaseShardedDS2> show collections
TEST
```

The writes / reads made to the collection is routed to the right database based on the database hint set in the context.

## Composite Sharding Strategy

!!! TODO

    Coming soon


