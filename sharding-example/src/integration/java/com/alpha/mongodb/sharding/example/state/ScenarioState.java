package com.alpha.mongodb.sharding.example.state;

import io.cucumber.java.Scenario;
import io.cucumber.spring.ScenarioScope;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

@ScenarioScope
@Data
@NoArgsConstructor(onConstructor = @__(@Autowired))
public class ScenarioState {
    private List<StepDefinitionState> stepDefinitionState = new ArrayList<>();

    private StepDefinitionState sharedStepDefinitionState;

    private Scenario scenario;

    public StepDefinitionState getPreviousStepDefinitionState() {
        return stepDefinitionState.get(stepDefinitionState.size() - 2);
    }

    public StepDefinitionState getCurrentStepDefinitionState() {
        return stepDefinitionState.get(stepDefinitionState.size() - 1);
    }

    public void addStep() {
        stepDefinitionState.add(new StepDefinitionState());
    }
}
