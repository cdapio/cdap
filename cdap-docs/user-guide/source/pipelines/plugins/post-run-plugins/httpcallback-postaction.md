# Http Callback


Description
-----------
Performs an HTTP request at the end of a pipeline run.


Use Case
--------
This action can be used when you want to perform an HTTP request at the end of a pipeline run.
For example, you may want to configure a pipeline so that a request is made to an alerts endpoint
if the pipeline run failed.


Properties
----------
**runCondition:** When to run the action. Must be one of 'completion', 'success', or 'failure'. Defaults to 'completion'.

- If set to 'completion', the action will be executed regardless of whether the pipeline run succeeded or failed.
- If set to 'success', the action will be executed only if the pipeline run succeeded.
- If set to 'failure', the action will be executed only if the pipeline run failed.

**url:** The URL to call.

**method:** The HTTP request method.

**body:** Optional request body.

**followRedirects:** Whether to automatically follow redirects. Defaults to true.

**connectTimeout:** The time in milliseconds to wait for a connection. Set to 0 for infinite. Defaults to 60000 (1 minute).

**numRetries:** The number of times the request should be retried if the request fails. Defaults to 0.

**requestHeaders:** An optional string of header values to send in each request where the keys and values are
delimited by a colon (":") and each pair is delimited by a newline ("\n").

Example
-------
This example performs an HTTP POST to http://monitoring.com/alerts whenever a run fails:

    {
        "name": "HttpCallback",
        "type": "postaction",
        "properties": {
            "url": "http://monitoring.com/alerts",
            "method": "POST"
        }
    }

---
- CDAP Pipelines Plugin Type: postaction
- CDAP Pipelines Version: 1.7.0
