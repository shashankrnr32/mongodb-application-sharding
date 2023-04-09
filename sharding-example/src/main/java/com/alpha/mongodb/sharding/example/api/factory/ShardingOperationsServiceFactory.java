package com.alpha.mongodb.sharding.example.api.factory;

import com.alpha.mongodb.sharding.example.api.enumeration.DataSourceType;
import com.alpha.mongodb.sharding.example.api.enumeration.ShardingType;
import com.alpha.mongodb.sharding.example.service.ShardedOperationsService;
import com.alpha.mongodb.sharding.example.service.impl.repository.ReactiveRepositoryCollectionShardedOperationService;
import com.alpha.mongodb.sharding.example.service.impl.repository.RepositoryCollectionShardedOperationService;
import com.alpha.mongodb.sharding.example.service.impl.template.ReactiveTemplateCollectionShardedOperationService;
import com.alpha.mongodb.sharding.example.service.impl.template.ReactiveTemplateDatabaseShardedOperationService;
import com.alpha.mongodb.sharding.example.service.impl.template.TemplateCollectionShardedOperationService;
import com.alpha.mongodb.sharding.example.service.impl.template.TemplateDatabaseShardedOperationService;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.NotImplementedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Log4j2
public class ShardingOperationsServiceFactory {

    @Autowired
    private TemplateCollectionShardedOperationService templateCollectionShardedOperationService;

    @Autowired
    private RepositoryCollectionShardedOperationService repositoryCollectionShardedOperationService;

    @Autowired
    private ReactiveRepositoryCollectionShardedOperationService reactiveRepositoryCollectionShardedOperationService;

    @Autowired
    private ReactiveTemplateCollectionShardedOperationService reactiveTemplateCollectionShardedOperationService;

    @Autowired
    private TemplateDatabaseShardedOperationService templateDatabaseShardedOperationService;

    @Autowired
    private ReactiveTemplateDatabaseShardedOperationService reactiveTemplateDatabaseShardedOperationService;

    public ShardedOperationsService get(ShardingType shardingType, DataSourceType dataSourceType, boolean reactive) {
        log.info("Getting service from shardingType={} and dataSourceType={} reactive={}", shardingType, dataSourceType, reactive);
        switch (shardingType) {
            case COLLECTION:
                switch (dataSourceType) {
                    case TEMPLATE:
                        return reactive ? reactiveTemplateCollectionShardedOperationService :
                                templateCollectionShardedOperationService;
                    case REPOSITORY:
                        return reactive ? reactiveRepositoryCollectionShardedOperationService :
                                repositoryCollectionShardedOperationService;
                    default:
                        throw new NotImplementedException();
                }
            case DATABASE:
                switch (dataSourceType) {
                    case TEMPLATE:
                        return reactive ? reactiveTemplateDatabaseShardedOperationService :
                                templateDatabaseShardedOperationService;
                    default:
                        throw new NotImplementedException();
                }
            default:
                throw new NotImplementedException();
        }
    }
}
