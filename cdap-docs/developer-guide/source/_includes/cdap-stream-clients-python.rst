CDAP Stream Client for Python
=============================

The Stream Client Python API is for managing Streams from Python
applications.

Supported Actions
-----------------

-  Create a Stream
-  Update TTL (time-to-live) for an existing Stream
-  Retrieve the current Stream TTL
-  Truncate an existing Stream (the deletion of all events that were
   written to the Stream)
-  Write an event to an existing Stream

Installation
------------

To install CDAP Stream Client, run

::

    $ pip install cdap-stream-client

Usage
-----

To use the Stream Client Python API, include these imports in your
Python script:

::

    from cdap_stream_client import Config
    from cdap_stream_client import StreamClient

Example
-------

Create StreamClient
~~~~~~~~~~~~~~~~~~~

Create a StreamClient instance with default parameters.

::

    stream_client = StreamClient()

Optional configurations that can be set (and their default values):

-  host: 'localhost'
-  port: 10000
-  ssl: False (set true to use HTTPS protocol)
-  ssl\_cert\_check: True (set False to suspend certificate checks; this
   allows self-signed certificates when SSL is True)
-  authClient: null (`CDAP Authentication
   Client <https://github.com/caskdata/cdap-clients/tree/develop/cdap-authentication-clients/python>`__
   to interact with a secure CDAP instance)

Example:

::

    config = Config()
    config.host = 'localhost'
    config.port = 10000
    config.ssl = True
    config.set_auth_client(authentication_client)

    stream_client = StreamClient(config)

Create Stream
~~~~~~~~~~~~~

Create a new Stream with the *stream-id* "newStreamName":

::

    stream_client.create("newStreamName")

**Notes:**

-  The *stream-id* should only contain ASCII letters, digits and
   hyphens.
-  If the Stream already exists, no error is returned, and the existing
   Stream remains in place.

Create StreamWriter
~~~~~~~~~~~~~~~~~~~

Create a ``StreamWriter`` instance for writing events to the Stream
"streamName":

::

    stream_writer = stream_client.create_writer("streamName")

Write Stream Events
~~~~~~~~~~~~~~~~~~~

To write new events to the Stream, use the ``write`` method of the
``StreamWriter`` class:

::

    def write(self, message, charset=None, headers=None)

Example:

::

    stream_promise = stream_writer.write("New stream event")

Truncate Stream
~~~~~~~~~~~~~~~

To delete all events that were written to the Stream *streamName*, use:

::

    stream_client.truncate("streamName")

Update Stream Time-to-Live (TTL)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Update TTL for the Stream *streamName*:

::

    stream_client.set_ttl("streamName", newTTL)

Get Stream Time-to-Live (TTL)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Get the current TTL value for the Stream *streamName*:

::

    ttl = stream_client.get_ttl("streamName")

StreamPromise
~~~~~~~~~~~~~

StreamPromise's goal is to implement deferred code execution.

For error handling, create a handler for each case and set it using the
``onResponse`` method. The error handling callback function is optional.

Example:

::

    def on_ok_response(response):
        ...
        parse response
        ...

    def on_error_response(response):
        ...
        parse response
        ...

    stream_promise = stream_writer.write("New stream event")
    stream_promise.on_response(on_ok_response, on_error_response)

