.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _procedures:

============================================
Procedures
============================================

.. include:: ../../../_common/_include/include-v260-deprecate-procedures.rst

To query CDAP and its Datasets and retrieve results, you can use Procedures.

Procedures allow you to make synchronous calls into CDAP from an external system
and perform server-side processing on-demand, similar to a stored procedure in a
traditional database.

Procedures are typically used to post-process data at query time. This
post-processing can include filtering, aggregating, or joins over
multiple Datasets—in fact, a Procedure can perform all the same
operations as a Flowlet with the same consistency and durability
guarantees. They are deployed into the same pool of application
containers as Flows, and you can run multiple instances to increase the
throughput of requests.

A Procedure implements and exposes a very simple API: a method name
(String) and arguments (map of Strings). This implementation is then
bound to a REST endpoint and can be called from any external system.

To create a Procedure you implement the ``Procedure`` interface, or more
conveniently, extend the ``AbstractProcedure`` class.

A Procedure is configured and initialized similarly to a Flowlet, but
instead of a process method you’ll define a handler method. Upon
external call, the handler method receives the request and sends a
response.

The initialize method is called when the Procedure handler is created.
It is not created until the first request is received for it.

The most generic way to send a response is to obtain a
``Writer`` and stream out the response as bytes. Make sure to close the
``Writer`` when you are done::

  import static co.cask.cdap.api.procedure.ProcedureResponse.Code.SUCCESS;
  ...
  class HelloWorld extends AbstractProcedure {

    @Handle("hello")
    public void wave(ProcedureRequest request,
                     ProcedureResponder responder) throws IOException {
      String hello = "Hello " + request.getArgument("who");
      ProcedureResponse.Writer writer =
        responder.stream(new ProcedureResponse(SUCCESS));
      writer.write(ByteBuffer.wrap(hello.getBytes())).close();
    }
  }

This uses the most generic way to create the response, which allows you
to send arbitrary byte content as the response body. In many cases, you
will actually respond with JSON. A CDAP
``ProcedureResponder`` has convenience methods for returning JSON maps::

  // Return a JSON map
  Map<String, Object> results = new TreeMap<String, Object>();
  results.put("totalWords", totalWords);
  results.put("uniqueWords", uniqueWords);
  results.put("averageLength", averageLength);
  responder.sendJson(results);

There is also a convenience method to respond with an error message::

  @Handle("getCount")
  public void getCount(ProcedureRequest request, ProcedureResponder responder)
                       throws IOException, InterruptedException {
    String word = request.getArgument("word");
    if (word == null) {
      responder.error(Code.CLIENT_ERROR,
                      "Method 'getCount' requires argument 'word'");
      return;
    }

.. rubric::  Examples of Using Procedures

Procedures are included in many CDAP :ref:`applications <apps-and-packs>`,
:ref:`tutorials <tutorials>`, :ref:`guides <guides-index>` and :ref:`examples <examples-index>`.

- The simplest example, :ref:`Hello World <examples-hello-world>`, demonstrates using a
  procedure to **retrieve a name from a dataset.**

- For **additional procedure examples,** see the 
  :ref:`Purchase <examples-purchase>`,
  :ref:`Spark K-Means <examples-spark-k-means>`, and
  :ref:`Spark Page Rank <examples-spark-page-rank>`
  examples.
