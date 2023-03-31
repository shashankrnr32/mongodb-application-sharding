package com.alpha.mongodb.sharding.example.api;

import com.alpha.mongodb.sharding.core.hint.ShardingHintManager;
import com.alpha.mongodb.sharding.example.api.enumeration.DataSourceType;
import com.alpha.mongodb.sharding.example.api.enumeration.ShardingType;
import com.alpha.mongodb.sharding.example.api.factory.ShardingOperationsServiceFactory;
import com.alpha.mongodb.sharding.example.entity.TestShardedEntity;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.*;

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
                                      @RequestParam DataSourceType dataSourceType) {
        if (StringUtils.isNotBlank(collectionShardHint)) {
            System.out.println(collectionShardHint);
            ShardingHintManager.setCollectionHint(collectionShardHint);
        }
        Optional<?> entityOptional = serviceFactory.get(shardingType, dataSourceType).findById(id);
        if (entityOptional.isPresent()) {
            return ResponseEntity.ok(entityOptional);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping(path = "/insert")
    public ResponseEntity<?> insert(@RequestBody TestShardedEntity testShardedEntity,
                                    @RequestParam ShardingType shardingType,
                                    @RequestParam DataSourceType dataSourceType) {
        serviceFactory.get(shardingType, dataSourceType).insert(testShardedEntity);
        return ResponseEntity.noContent().build();
    }

}
