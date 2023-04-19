package com.alpha.mongodb.sharding.example.state;

import lombok.Data;
import okhttp3.Request;
import okhttp3.Response;

@Data
public class StepDefinitionState {
    private Request request;
    private Object requestBody;
    private Response response;
}
