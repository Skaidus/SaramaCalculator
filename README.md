# Sarama Calculator
A simple calculator service built around [Sarama](https://github.com/Shopify/sarama), a Go client library for [Apache Kafka](https://kafka.apache.org/), to ilustrate the usage of available mocks.

Calculator reads from `commandsTopic` to update its inner value, posting the result in `resultsTopic`. 

The same test suite is implemented in two exclusive ways:

* **calculator_mocks_test**: uses `sarama/mocks` elements, which covers producer and consumer mocking. No broker is involved.
* **calculator_sarama_test**: uses `sarama` mocks that for some reason are not included in the submodule. It offers a Mock Broker in which you can match requests to responses. In addition you can pass response streams that are consumed in that order.