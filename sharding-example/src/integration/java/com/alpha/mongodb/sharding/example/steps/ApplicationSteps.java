package com.alpha.mongodb.sharding.example.steps;

import com.alpha.mongodb.sharding.example.CucumberTestConstants;
import com.alpha.mongodb.sharding.example.state.ScenarioState;
import com.alpha.mongodb.sharding.example.utils.ExampleServiceClientUtils;
import io.cucumber.java.BeforeStep;
import io.cucumber.java.Scenario;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import lombok.extern.log4j.Log4j2;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.ThreadContext;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@Log4j2
public class ApplicationSteps {

    @Autowired
    private OkHttpClient okHttpClient;

    @Autowired
    private ScenarioState scenarioState;

    @Before
    public void beforeScenario(Scenario scenario) {
        ThreadContext.put(CucumberTestConstants.SCOPE, CucumberTestConstants.CUKE);
        scenarioState.setScenario(scenario);
    }

    @BeforeStep
    public void beforeStep() {
        scenarioState.addStep();
    }

    @When("I call the health API of the sharding example application")
    public void whenThatShardingExampleApplicationIsUp() throws IOException {
        HttpUrl httpUrl = ExampleServiceClientUtils.baseUrlBuilder()
                .addPathSegment("actuator").addPathSegment("health").build();
        System.out.println(httpUrl);
        Request request = new Request.Builder().url(httpUrl).get().build();
        Response healthApiResponse = okHttpClient.newCall(request).execute();

        scenarioState.getCurrentStepDefinitionState().setRequest(request);
        scenarioState.getCurrentStepDefinitionState().setResponse(healthApiResponse);
    }

    @Then("I get a response with status code {int}")
    public void thenIGetResponseWithStatusCode(int status) {
        assertEquals(status, scenarioState.getPreviousStepDefinitionState().getResponse().code());
    }
}
