package com.alpha.mongodb.sharding.core.template;

import com.mongodb.client.MongoClient;
import com.mongodb.client.result.DeleteResult;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.query.Query;

public class ExtendedMongoTemplate extends MongoTemplate {
    public ExtendedMongoTemplate(MongoClient mongoClient, String databaseName) {
        super(mongoClient, databaseName);
    }

    public ExtendedMongoTemplate(MongoDatabaseFactory mongoDbFactory) {
        super(mongoDbFactory);
    }

    public ExtendedMongoTemplate(MongoDatabaseFactory mongoDbFactory, MongoConverter mongoConverter) {
        super(mongoDbFactory, mongoConverter);
    }

    public DeleteResult removeOne(Query query, Class<?> entityClass, String collectionName) {
        return super.doRemove(collectionName, query, entityClass, false);
    }
}
