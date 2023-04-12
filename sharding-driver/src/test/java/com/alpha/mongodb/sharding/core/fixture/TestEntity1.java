package com.alpha.mongodb.sharding.core.fixture;

import lombok.Data;
import lombok.experimental.FieldNameConstants;

@Data
@FieldNameConstants
public class TestEntity1 {
    private String id;

    private String indexedField;
}