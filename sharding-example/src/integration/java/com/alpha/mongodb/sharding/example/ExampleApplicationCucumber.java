package com.alpha.mongodb.sharding.example;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(features = "src/integration/resources/features", plugin = {"pretty", "html:target/site/cucumber.html"})
public class ExampleApplicationCucumber {
}
