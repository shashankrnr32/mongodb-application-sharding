Feature: Verify that the sharding example application comes up

  Scenario: Sharding Example comes up successfully when the application is run
    When I call the health API of the sharding example application
    Then I get a response with status code 200