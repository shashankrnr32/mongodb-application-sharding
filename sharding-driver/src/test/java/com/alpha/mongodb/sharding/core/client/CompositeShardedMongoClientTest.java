package com.alpha.mongodb.sharding.core.client;

import com.alpha.mongodb.sharding.core.configuration.CompositeShardingOptions;
import com.mongodb.client.MongoClient;
import org.junit.Test;

import java.util.stream.IntStream;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class CompositeShardedMongoClientTest {

    CompositeShardingOptions compositeShardingOptions =
            CompositeShardingOptions.withIntegerStreamHints(IntStream.range(0, 3), IntStream.range(0, 3));

    MongoClient mongoClient = mock(MongoClient.class);

    @Test
    public void testConstructor() {
        assertNotNull(new CompositeShardedMongoClient(mongoClient, compositeShardingOptions));
    }

}