# Collection Sharding

## With Spring / Spring Boot

### How to use?

1. Create a custom `MongoTemplate` bean using the snippet given below

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
2. Implement your entity classes from `CollectionShardedEntity`
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

If the sharding hint is not set, methods will throw a `UnresolvableShardException`
.
