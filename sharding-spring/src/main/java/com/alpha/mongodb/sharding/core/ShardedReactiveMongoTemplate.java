package com.alpha.mongodb.sharding.core;

import com.alpha.mongodb.sharding.core.assitant.ShardingAssistant;
import com.alpha.mongodb.sharding.core.callback.HintResolutionCallback;
import com.alpha.mongodb.sharding.core.callback.HintResolutionCallbacks;
import com.alpha.mongodb.sharding.core.configuration.ShardingOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import lombok.Getter;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoConverter;

import java.util.Set;
import java.util.function.Consumer;

/**
 * Abstract Base Sharded Reactive Mongo Template
 *
 * @author Shashank Sharma
 */
public abstract class ShardedReactiveMongoTemplate extends ReactiveMongoTemplate implements ShardingAssistant {

    @Getter
    private final ShardingOptions shardingOptions;
    @Getter
    private final HintResolutionCallbacks hintResolutionCallbacks;

    protected ShardedReactiveMongoTemplate(MongoClient mongoClient, String databaseName, final ShardingOptions shardingOptions) {
        super(mongoClient, databaseName);
        this.shardingOptions = shardingOptions;
        hintResolutionCallbacks = new HintResolutionCallbacks(
                (Set<HintResolutionCallback<?>>) shardingOptions.getHintResolutionCallbacks());
    }

    protected ShardedReactiveMongoTemplate(ReactiveMongoDatabaseFactory mongoDbFactory, final ShardingOptions shardingOptions) {
        super(mongoDbFactory);
        this.shardingOptions = shardingOptions;
        hintResolutionCallbacks = new HintResolutionCallbacks(
                (Set<HintResolutionCallback<?>>) shardingOptions.getHintResolutionCallbacks());
    }

    protected ShardedReactiveMongoTemplate(ReactiveMongoDatabaseFactory mongoDbFactory, MongoConverter mongoConverter, final ShardingOptions shardingOptions) {
        super(mongoDbFactory, mongoConverter);
        this.shardingOptions = shardingOptions;
        hintResolutionCallbacks = new HintResolutionCallbacks(
                (Set<HintResolutionCallback<?>>) shardingOptions.getHintResolutionCallbacks());
    }

    protected ShardedReactiveMongoTemplate(ReactiveMongoDatabaseFactory mongoDatabaseFactory, MongoConverter mongoConverter, Consumer<Throwable> subscriptionExceptionHandler, final ShardingOptions shardingOptions) {
        super(mongoDatabaseFactory, mongoConverter, subscriptionExceptionHandler);
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
