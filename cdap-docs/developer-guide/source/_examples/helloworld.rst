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

The `` Greeting`` Procedure
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

Building and Running the Application and Example
................................................

.. highlight:: console

In the remainder of this document, we refer to the Standalone CDAP as "CDAP", and the
example code that is running on it as an "Application".

We show the Windows prompt as ``~SDK>`` to indicate a command prompt opened in the SDK directory.

In this example, you need to build the app from source and then deploy the compiled JAR file.
You start the CDAP, deploy the app, start the Flow and then run the example by
injecting a name into the stream. Then you can invoke the procedure to recevive a greeting.

When finished, stop the Application as described below.

Building the Purchase Application
+++++++++++++++++++++++++++++++++

From the project root, build ``HelloWorld`` with the
`Apache Maven <http://maven.apache.org>`__ command::

	$ mvn clean package

(If you modify the code and would like to rebuild the Application, you can
skip the tests by using the command::

	$ mvn -Dmaven.test.skip=true clean package


Deploying and Starting the Application
++++++++++++++++++++++++++++++++++++++

Make sure an instance of the CDAP is running and available.
From within the SDK root directory, this command will start CDAP in local mode::

	$ ./bin/cdap.sh start

On Windows::

	~SDK> bin\cdap.bat start

From within the CDAP Console (`http://localhost:9999/ <http://localhost:9999/>`__ in local mode):

#. Drag and drop the Application .JAR file (``target/HelloWorld-<version>.jar``)
   onto your browser window.
   Alternatively, use the *Load App* button found on the *Overview* of the CDAP Console.
#. Once loaded, select the ``HelloWorld`` Application from the list.
   On the Application's detail page, click the *Start* button on **both** the *Process* and *Query* lists.

On Windows:

#. To deploy the App JAR file, run ``~SDK> bin\app-manager.bat deploy`` or drag and drop the
   Application .JAR file (``target/HelloWorld-<version>.jar`` onto your browser window.
   (Currently, the *Load App* button does not work under Windows.)
#. To start the App, run ``~SDK> bin\app-manager.bat start``

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


Stopping the Application
++++++++++++++++++++++++

Either:

- On the Application detail page of the CDAP Console,
  click the *Stop* button on **both** the *Process* and *Query* lists;

or:

- Run ``$ ./bin/app-manager.sh --action stop``

  On Windows, run ``~SDK> bin\app-manager.bat stop``

.. highlight:: java

Downloading the Example
.......................

This example (and more!) is included with our `software development kit <http://cask.co/download>`__.

