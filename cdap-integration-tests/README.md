# CDAP Integration Tests

## Running

```
mvn install -DskipTests -P integration-tests -pl cdap-integration-tests -am && \
mvn test -P integration-tests -pl cdap-integration-tests
```

This will automatically start up a new CDAP standalone instance for testing.

## Running against provided CDAP instance

```
mvn install -DskipTests -P integration-tests -pl cdap-integration-tests -am && \
mvn test -P integration-tests -pl cdap-integration-tests -DargLine="-DinstanceUri=<YOUR CDAP INSTANCE URI>"
```

For example, to run against `http://example.com:10000`:

```
mvn install -DskipTests -P integration-tests -pl cdap-integration-tests -am && \
mvn test -P integration-tests -pl cdap-integration-tests -DargLine="-DinstanceUri=http://example.com:10000"
```
