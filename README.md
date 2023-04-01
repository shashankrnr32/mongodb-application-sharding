# mongodb-application-sharding

[![Java CI with Maven](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven.yml/badge.svg)](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven.yml)
[![Maven Package](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven-publish.yml/badge.svg)](https://github.com/shashankrnr32/mongodb-application-sharding/actions/workflows/maven-publish.yml)

Mongo DB Application Sharding allows you to shard your Mongo DB cluster from your application using different
strategies.

Application Sharding Strategies supported by the library

1. [Collection](#collection-sharding-strategy)
2. Database
3. Composite

## Get Started

To use this library in your project, just add the package `sharding-core`
as a dependency to your project.

```xml
<!-- sharding-core -->
<dependency>
    <groupId>com.alpha</groupId>
    <artifactId>sharding-core</artifactId>
    <version>${mongodb.sharding.version}</version>
</dependency>
```

## Collection Sharding Strategy

Sharding strategy where data is divided into multiple collections in a single database identified by a shardHint
(usually a suffix to the collection name)

```text
ds0> show collections
TEST_0
TEST_1
TEST_2
```

### How to use?

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

4. Voila!.

### Sharding Hint

In order to route the write queries, the entities are supposed to implement from CollectionShardedEntity. But, the find
queries can take place with different criterion, with different fields. In order to route the find query to the right
collection, sharding hint is used.

```java
import java.util.Optional;

public class TestRepository {

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

## Author

[Shashank Sharma](https://github.com/shashankrnr32)

