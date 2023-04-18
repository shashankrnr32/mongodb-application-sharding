package com.alpha.mongodb.sharding.core.template;

import com.alpha.mongodb.sharding.core.assitant.ShardingAssistant;
import com.alpha.mongodb.sharding.core.callback.HintResolutionCallback;
import com.alpha.mongodb.sharding.core.callback.HintResolutionCallbacks;
import com.alpha.mongodb.sharding.core.configuration.ShardingOptions;
import com.mongodb.client.MongoClient;
import lombok.Getter;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import java.util.Set;

/**
 * Abstract Base Sharded Mongo Template
 *
 * @author Shashank Sharma
 */
public abstract class ShardedMongoTemplate extends ExtendedMongoTemplate implements ShardingAssistant {

    @Getter
    private final ShardingOptions shardingOptions;
    @Getter
    private final HintResolutionCallbacks hintResolutionCallbacks;

    protected ShardedMongoTemplate(MongoClient mongoClient, String databaseName, final ShardingOptions shardingOptions) {
        super(mongoClient, databaseName);
        this.shardingOptions = shardingOptions;
        hintResolutionCallbacks = new HintResolutionCallbacks(
                (Set<HintResolutionCallback<?>>) shardingOptions.getHintResolutionCallbacks());
    }

    protected ShardedMongoTemplate(MongoDatabaseFactory mongoDbFactory, final ShardingOptions shardingOptions) {
        super(mongoDbFactory);
        this.shardingOptions = shardingOptions;
        hintResolutionCallbacks = new HintResolutionCallbacks(
                (Set<HintResolutionCallback<?>>) shardingOptions.getHintResolutionCallbacks());
    }

    ShardedMongoTemplate(MongoDatabaseFactory mongoDbFactory, MongoConverter mongoConverter, final ShardingOptions shardingOptions) {
        super(mongoDbFactory, mongoConverter);
        this.shardingOptions = shardingOptions;
        hintResolutionCallbacks = new HintResolutionCallbacks(
                (Set<HintResolutionCallback<?>>) shardingOptions.getHintResolutionCallbacks());
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        super.setApplicationContext(applicationContext);
        hintResolutionCallbacks.discover(applicationContext);
    }
}
