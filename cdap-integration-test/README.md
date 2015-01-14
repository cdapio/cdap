# CDAP Integration Test Framework

Users can use `IntegrationTestBase` to write tests that run against a framework-provided standalone CDAP instance
or a remote CDAP instance.

## Running tests using the framework-provided standalone CDAP instance

```
cd <your-test-module>
mvn test
```

## Running tests against a remote CDAP instance

```
cd <your-test-module>
mvn test -DargLine="-DinstanceUri=<instance URI> -DaccessToken=<access token>"
```

* `<instance URI>` is the URI used to connect to your CDAP router (e.g. http://example.com:10000)
* `<access token>` is the access token obtained from your CDAP authentication server by logging in as user
  * Note: This is unnecessary in a non-secure CDAP instance

For example, to run tests against a CDAP instance at `http://example.com:10000` with access token `abc123`:

```
mvn test -DargLine="-DinstanceUri=http://example.com:10000 -DaccessToken=abc123"
```
