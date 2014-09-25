
.. :Author: Cask Data, Inc.
   :Description: Cask Data Application Platform Hello World Application
   :Copyright: Copyright © 2014 Cask Data, Inc.

.. _hello-world:

HelloWorld
----------

The most simple Cask Data Application Platform (CDAP) Example.

Overview
........

This application uses one Stream, one Dataset, one Flow and one Procedure to implement the classic "Hello World".

- A stream to send names to;
- A flow with a single flowlet that reads the stream and stores each name in a KeyValueTable; and
- A procedure that reads the name from the KeyValueTable and prints 'Hello [Name]!'.

The ``WhoFlow``
+++++++++++++++

This is a trivial flow with a single flowlet named ``NamSaver``::

  public static class WhoFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with().
        setName("WhoFlow").
        setDescription("A flow that collects names").
        withFlowlets().add("saver", new NameSaver()).
        connect().fromStream("who").to("saver").
        build();
    }
  }

The flowlet uses a dataset of type ``KeyValueTable`` to store the names it reads from the stream. Every time a new
name is received, it is stored in the table under the key ``name``-and it overwrites any name that was previously
stored::

  /**
   * Sample Flowlet.
   */
  public static class NameSaver extends AbstractFlowlet {

    static final byte[] NAME = { 'n', 'a', 'm', 'e' };

    @UseDataSet("whom")
    KeyValueTable whom;
    Metrics flowletMetrics;

    @ProcessInput
    public void process(StreamEvent event) {
      byte[] name = Bytes.toBytes(event.getBody());
      if (name != null && name.length > 0) {
        whom.write(NAME, name);
      }
      if (name.length > 10) {
        flowletMetrics.count("names.longnames", 1);
      }
      flowletMetrics.count("names.bytes", name.length);
    }
  }

Note that the flowlet also emits metrics: Every time a name longer than 10 characters is received,
the counter ``names.longnames`` is incremented by one, and the metric ``names.bytes`` is incremented
by the length of the name. We will see below how to retrieve these metrics using the CDAP Client API.

The ``Greeting`` Procedure
+++++++++++++++++++++++++++

This procedure has a single handler method called ``greet`` that does not except arguments. When invoked, it
reads the name stored by the ``NameSaver`` from the key-value table. It return a simple gretting with that name::

  public static class Greeting extends AbstractProcedure {

    @UseDataSet("whom")
    KeyValueTable whom;
    Metrics procedureMetrics;

    @Handle("greet")
    public void greet(ProcedureRequest request, ProcedureResponder responder) throws Exception {
      byte[] name = whom.read(NameSaver.NAME);
      String toGreet = name != null ? new String(name) : "World";
      if (toGreet.equals("Jane Doe")) {
        procedureMetrics.count("greetings.count.jane_doe", 1);
      }
      responder.sendJson(new ProcedureResponse(SUCCESS), "Hello " + toGreet + "!");
    }
  }

Deploy and start the application as described in  :ref:`Build, Deploy and start <convention>`

Running the Example
+++++++++++++++++++

Injecting a Name
################

In the Application's detail page, under Process, click on WhoFlow. This takes you to the flow details page.
Now click on the "who" stream on the left side of the flow visualization, which brings up a pop-up window.
Enter a name and click the Inject button. After you close the pop-up window, you will see that the counters
for both the stream and the "saver" flowlet increase to 1. You can repeat this step to enter more names, but
remember that only the last name ist stored in the key-value table.

Using the Procedure
###################

Go back to the Application's detail page, and under Query, click on the Greeting procedure. Now you can make a
request to the procedure: Enter "greet" for the method and click the Execute button. At the bottom of the page you
will see the procedure's response. If the last name you entered is Tom, this will be "Hello, Tom!".

Retrieving Metrics
##################

You can now query the metrics that are emitted by the flow. To see the value of the ``names.bytes`` metric,
you can make an HTTP request to the Metrics API using curl::

  $ curl http://localhost:10000/v2/metrics/user/apps/HelloWorld/flows/WhoFlow/flowlets/saver/names.bytes?aggregate=true
  {"data":3}

Once done, You can stop the application as described in :ref:`Stop Application <stop-application>`
