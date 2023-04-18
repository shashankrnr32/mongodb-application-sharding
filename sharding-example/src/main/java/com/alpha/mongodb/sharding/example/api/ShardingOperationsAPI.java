package com.alpha.mongodb.sharding.example.api;

import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.alpha.mongodb.sharding.example.api.enumeration.DataSourceType;
import com.alpha.mongodb.sharding.example.api.enumeration.ShardingType;
import com.alpha.mongodb.sharding.example.api.factory.ShardingOperationsServiceFactory;
import com.alpha.mongodb.sharding.example.api.models.EntityDTO;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@Log4j2
@RestController
@RequestMapping("/v1/ops")
public class ShardingOperationsAPI {

    @Autowired
    private ShardingOperationsServiceFactory serviceFactory;

    @GetMapping(path = "/find/id/{id}")
    public ResponseEntity<?> findById(@PathVariable String id,
                                      @RequestParam @Nullable String collectionShardHint,
                                      @RequestParam @Nullable String databaseShardHint,
                                      @RequestParam ShardingType shardingType,
                                      @RequestParam DataSourceType dataSourceType,
                                      @RequestParam boolean reactive) {
        if (StringUtils.isNotBlank(collectionShardHint)) {
            ShardingHintManager.setCollectionHint(collectionShardHint);
            ShardingHintManager.setDatabaseHint(databaseShardHint);
        }
        Optional<?> entityOptional = serviceFactory.get(shardingType, dataSourceType, reactive).findById(id);
        if (entityOptional.isPresent()) {
            return ResponseEntity.ok(entityOptional);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping(path = "/insert")
    public ResponseEntity<?> insert(@RequestBody EntityDTO testShardedEntity,
                                    @RequestParam ShardingType shardingType,
                                    @RequestParam DataSourceType dataSourceType,
                                    @RequestParam boolean reactive) {
        EntityDTO persistedEntityDTO = serviceFactory.get(shardingType, dataSourceType, reactive).insert(testShardedEntity);
        return ResponseEntity.ok(persistedEntityDTO.getId());
    }

    @GetMapping(path = "/find/indexed/{value}")
    public ResponseEntity<?> findByIndexedField(@PathVariable String value,
                                                @RequestParam @Nullable String collectionShardHint,
                                                @RequestParam @Nullable String databaseShardHint,
                                                @RequestParam ShardingType shardingType,
                                                @RequestParam DataSourceType dataSourceType,
                                                @RequestParam boolean reactive) {
        if (StringUtils.isNotBlank(collectionShardHint)) {
            ShardingHintManager.setCollectionHint(collectionShardHint);
            ShardingHintManager.setDatabaseHint(databaseShardHint);
        }
        Optional<?> entityOptional = serviceFactory.get(shardingType, dataSourceType, reactive).findByIndexedField(value);
        if (entityOptional.isPresent()) {
            return ResponseEntity.ok(entityOptional);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

}
