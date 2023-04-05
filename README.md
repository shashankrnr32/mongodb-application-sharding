# mongodb-application-sharding

[![Java CI with Maven](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven.yml/badge.svg)](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven.yml)
[![Maven Package](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven-publish.yml/badge.svg)](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven-publish.yml)
[![codecov](https://codecov.io/gh/shashankrnr32/mongodb-application-sharding/branch/main/graph/badge.svg?token=U51FX5G10S)](https://codecov.io/gh/shashankrnr32/mongodb-application-sharding)

[![SonarCloud](https://sonarcloud.io/images/project_badges/sonarcloud-black.svg)](https://sonarcloud.io/summary/new_code?id=shashankrnr32_mongodb-application-sharding)

Mongo DB Application Sharding allows you to shard your Mongo DB cluster from your application using different
strategies. This project is inspired by [Apache's shardingsphere](https://github.com/apache/shardingsphere) which
enables the users to shard the relational databases through the application.

Application Sharding Strategies supported by the library

1. [Collection](#collection-sharding-strategy)
2. [Database](#database-sharding-strategy)
3. [Composite](#composite-sharding-strategy)

## Get Started

To use this library in your project, just add the package `sharding-core`
as a dependency to your project.

```xml
<!-- sharding-core -->
<dependency>
    <groupId>com.alpha.mongodb</groupId>
    <artifactId>sharding-core</artifactId>
    <version>${mongodb.sharding.version}</version>
</dependency>
```

### Collection Sharding Strategy

Sharding strategy where data is divided into multiple collections in a single database identified by a shardHint
(usually a suffix to the collection name)

```shell
ds0> show collections
TEST_0
TEST_1
TEST_2
```

#### How to use?

1. Create a custom `MongoTemplate` bean using the snippet given in
   [this example](sharding-example/src/main/java/com/alpha/mongodb/sharding/example/configuration/CollectionShardedMongoConfiguration.java)
   .

```java
package com.alpha.mongodb.sharding.example.configuration;

@Configuration
public class CollectionShardedMongoConfiguration {

    @Bean("collectionShardedMongoTemplate")
    public MongoTemplate collectionShardedMongoTemplate(
            @Autowired MongoDatabaseFactory collectionShardedMongoDbFactory) {

        CollectionShardingOptions shardingOptions =
                CollectionShardingOptions.withIntegerStreamHints(
                        IntStream.range(0, 3));
        return new CollectionShardedMongoTemplate(
                collectionShardedMongoDbFactory(), shardingOptions);
    }
}
```

2. Implement your entity classes from
   [`CollectionShardedEntity`](sharding-core/src/main/java/com/alpha/mongodb/sharding/core/entity/CollectionShardedEntity.java)
   like the example given below. This will ensure the writes to the entity gets routed to the right collection.

```java
// Sample TestShardedEntity
@Document("TEST")
@Data
@FieldNameConstants
public class TestShardedEntity implements CollectionShardedEntity {

    @Id
    private String id;

    @Indexed(unique = true)
    private String indexedField;

    @Override
    public String resolveCollectionHint() {
        return String.valueOf(indexedField.charAt(1) - '0');
    }
}
```

3. Autowire the `collectionShardedMongoTemplate` and use it wherever required.

#### Sharding Hint

In order to route the write queries, the entities are supposed to implement from CollectionShardedEntity. But, the find
queries can take place with different criterion, with different fields. In order to route the find query to the right
collection, sharding hint is used.

```java
import java.util.Optional;

public class TestRepository {

    @Autowired
    @Qualifier("collectionShardedMongoTemplate")
    private MongoTemplate collectionShardedMongoTemplate;

    public Optional<TestShardEntity> findById() {
        ShardingHintManager.setCollectionHint("3");
        return Optional.ofNullable(
                collectionShardedMongoTemplate.findById(
                        "6427b9327a2cad734d5ff051",
                        TestShardEntity.class));
    }
}
```

If the sharding hint is not set, methods will throw
a [`UnresolvableShardException`](sharding-core/src/main/java/com/alpha/mongodb/sharding/core/exception/UnresolvableShardException.java)
.

### Database Sharding Strategy

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

#### How to use?

1. Create a custom `MongoTemplate` bean using the snippet given in
   [this example](sharding-example/src/main/java/com/alpha/mongodb/sharding/example/configuration/DatabaseShardedMongoConfiguration.java)
   .

```java

@Configuration
public class DatabaseShardedMongoConfiguration {

    private static final String SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS0 = "spring.mongodb.sharded.database.ds0.uri";
    private static final String SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS1 = "spring.mongodb.sharded.database.ds1.uri";
    private static final String SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS2 = "spring.mongodb.sharded.database.ds2.uri";

    @Autowired
    private Environment environment;

    @Bean
    public MongoDatabaseFactory ds0ShardedMongoDbFactory() {
        return new SimpleMongoClientDatabaseFactory(environment.getProperty(SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS0));
    }

    @Bean
    public MongoDatabaseFactory ds1ShardedMongoDbFactory() {
        return new SimpleMongoClientDatabaseFactory(environment.getProperty(SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS1));
    }

    @Bean
    public MongoDatabaseFactory ds2ShardedMongoDbFactory() {
        return new SimpleMongoClientDatabaseFactory(environment.getProperty(SPRING_MONGO_DB_URI_DATABASE_SHARDED_DS2));
    }

    @Bean("databaseShardedMongoTemplate")
    public MongoTemplate databaseShardedMongoTemplate() {
        DatabaseShardingOptions shardingOptions = DatabaseShardingOptions.withIntegerStreamHints(IntStream.range(0, 3));
        Map<String, MongoDatabaseFactory> factoryMap = new HashMap<String, MongoDatabaseFactory>() {{
            put(String.valueOf(0), ds0ShardedMongoDbFactory());
            put(String.valueOf(1), ds1ShardedMongoDbFactory());
            put(String.valueOf(2), ds2ShardedMongoDbFactory());
        }};
        return new DatabaseShardedMongoTemplate(factoryMap, shardingOptions);
    }
}
```

2. Implement your entity classes from
   [`DatabaseShardedEntity`](sharding-core/src/main/java/com/alpha/mongodb/sharding/core/entity/DatabaseShardedEntity.java)
   like the example given below. This will ensure the writes to the entity gets routed to the right database.

```java
// Sample TestShardedEntity
@Document("TEST")
@Data
@FieldNameConstants
public class TestShardedEntity implements CollectionShardedEntity {

    @Id
    private String id;

    @Indexed(unique = true)
    private String indexedField;

    @Override
    public String resolveDatabaseHint() {
        return String.valueOf(indexedField.charAt(0) - '0');
    }
}
```

3. Autowire the `databaseShardedMongoTemplate` and use it wherever required.

#### Sharding Hint

In order to route the write queries, the entities are supposed to implement from DatabaseShardedEntity. But, the find
queries can take place with different criterion, with different fields. In order to route the find query to the right
database, sharding hint is used.

```java
import java.util.Optional;

public class TestRepository {

    @Autowired
    @Qualifier("databaseShardedMongoTemplate")
    private MongoTemplate databaseShardedMongoTemplate;

    public Optional<TestShardEntity> findById() {
        ShardingHintManager.setDatabaseHint("3");
        return Optional.ofNullable(
                databaseShardedMongoTemplate.findById(
                        "6427b9327a2cad734d5ff051",
                        TestShardEntity.class));
    }
}
```

If the sharding hint is not set, methods will throw
a [`UnresolvableShardException`](sharding-core/src/main/java/com/alpha/mongodb/sharding/core/exception/UnresolvableShardException.java)
.

### Composite Sharding Strategy

Coming soon...!

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

