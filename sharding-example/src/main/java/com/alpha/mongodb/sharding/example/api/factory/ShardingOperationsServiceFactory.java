package com.alpha.mongodb.sharding.example.api.factory;

import com.alpha.mongodb.sharding.example.api.enumeration.DataSourceType;
import com.alpha.mongodb.sharding.example.api.enumeration.ShardingType;
import com.alpha.mongodb.sharding.example.service.ShardedOperationsService;
import com.alpha.mongodb.sharding.example.service.impl.template.CollectionShardedOperationService;
import org.apache.commons.lang3.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ShardingOperationsServiceFactory {

    @Autowired
    private CollectionShardedOperationService collectionShardedOperationService;

    public ShardedOperationsService get(ShardingType shardingType, DataSourceType dataSourceType) {
        switch (shardingType) {
            case COLLECTION:
                switch (dataSourceType) {
                    case TEMPLATE:
                        return collectionShardedOperationService;
                    default:
                        throw new NotImplementedException();
                }
            default:
                throw new NotImplementedException();
        }
    }
}
