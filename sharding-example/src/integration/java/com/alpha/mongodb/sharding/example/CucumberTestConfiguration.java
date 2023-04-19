package com.alpha.mongodb.sharding.example;

import com.alpha.mongodb.sharding.example.state.ScenarioState;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import static io.cucumber.spring.CucumberTestContext.SCOPE_CUCUMBER_GLUE;

@Configuration
public class CucumberTestConfiguration {

    @Bean
    @Scope(SCOPE_CUCUMBER_GLUE)
    public ScenarioState featureState() {
        return new ScenarioState();
    }

    @Bean
    public OkHttpClient httpClient() {
        return new OkHttpClient();
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

}
