.. :Author: Continuuity, Inc.
   :Description: Introducing new developers to Continuuity Reactor

===============================
Quick Start
===============================

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

This Quick Start will guide you through installing Continuuity Reactor,
running an example that counts HTTP status codes
and then modifying the example's Java code to include counting client IP addresses.

Checkout the Application
------------------------

We've pre-deployed one of the example applications to the Continuuity Reactor.
When you startup the Reactor, you'll be guided through viewing the application,
injecting an Event into a Flow and querying a Procedure to obtain results.

The example code for the *ResponseCodeAnalytics* that we'll be using is located in ``/examples/ResponseCodeAnalytics``.

Step 1: Installation and Startup
---------------------------------
Download and unpack the SDK from `Continuuity.com </download>`_.

Start the Reactor from a command line in the SDK directory::

	$ bin/reactor.sh start

Or, on Windows::

	> bin\reactor.bat start

View the Reactor Dashboard in a browser window::

	http://localhost:9999

Take the tour: you will be guided through the Dashboard, injecting HTTP log events and querying a Procedure to get a count of status codes.

Step 2: The Dashboard
----------------------
When you first open the Dashboard, you'll be greeted with:

.. image:: _images/quickstart/overview.png
   :width: 400px

Click on the name of the Application (**ResponseCodeAnalytics**) to view the running Application. The Application has each
of the Reactor's components:

- Collect: a Stream *logEventStream*
- Process: a Flow *LogAnalyticsFlow*
- Store: a Table *statusCodeTable*
- Query: a Procedure *StatusCodeProcedure*

Notice that Collect and Store elements are named using "camel-case",
while Process and Query elements are named using "leading-case"; the former indicates
the code is using an instance of a class,
while the latter indicates that the code is implementing a class.

Step 3: Inject Data
-------------------
Click on the Flow name (**LogAnalyticsFlow**), and you will be guided through clicking on a Stream icon
to add an Event to the Flow. We've pre-populated the injector with an Apache log line such as::

	165.225.156.91 - - [09/Jan/2014:21:28:53 -0400] "GET /index.html HTTP/1.1" 200 225 "http://continuuity.com" "Mozilla/4.08 [en] (Win98; I ;Nav)"

Once you inject the Event, if you leave the dialog box open, you can see it passing through the Flow on the real-time graph of *Events Per Second*. (Depending on the load on your computer, it might take as long as second for the event to show up.) Close the dialog when you're done and click on the Application link in the
upper-left corner to return to the Application Overview.

Step 4: Query Procedure
-----------------------
Now let's see the results of our event.

Click on the name of the Procedure (**StatusCodeProcedure**) to go to the Query view. You will be guided
through entering a method name (``getCounts``) and the method results will be returned, such as::

	{"200":1}

This indicates that for status code *200* ("The request has succeeded"), 1 event was received.
If you performed more than one injection, your results will be different. The total should
match the number of injections you made.

Step 5: Modify The Code
-----------------------
Now let's try something different. In addition to being able to count the number of hits on
different status codes, we'd like to be able to list all the unique client IP addresses and their counts.

We'll update the code, stop the Application, redeploy it, restart its Flow and Procedure,
and inject additional events. We'll test our modifications to the Procedure to see new statistics.

To build the example, we've included a `maven <http://maven.apache.org>`_ file. It's located in
the Application's source code directory (``/examples/ResponseCodeAnalytics``). Run (from within the source
code directory ``/examples/ResponseCodeAnalytics``) the command::

	mvn clean package

to build the .JAR file for deploying the application.

(On Windows, `these instructions <http://maven.apache.org/guides/getting-started/windows-prerequisites.html>`__
may help with problems using maven.)

Open the source file (*ResponseCodeAnalyticsApp.java*) in your preferred editor,
and make the following changes.

After the line ``private OutputEmitter<Integer> output;`` insert this code::

	// Emitter for emitting client IP address to the next Flowlet
	@Output("clientIps")
	private OutputEmitter<String> outputClientIP;

This will define an emitter *clientIps* that we'll send the client IPs out on.

After the line ``output.emit(Integer.parseInt(matcher.group(6)));`` insert::

	// Emit the IP address to the next connected Flowlet
	outputClientIP.emit(matcher.group(1));

This will implement the emitter *clientIps* and send the client IP address to the
downstream Flowlet.

Add to the class ``LogCountFlowlet`` the following ``count`` method::

    // Annotation indicates that this method can process incoming data
    @ProcessInput
    public void count(String ip) {
    
      // Increment the number of occurrences of the client IP address by 1
      statusCodes.increment(Bytes.toBytes("clientIPKey"), Bytes.toBytes(ip), 1L);
    }

This new method that will count IP address occurrences.

To the class ``StatusCodeProcedure``, add the following ``getClientIPCounts`` method::

    @Handle("getClientIPCounts")
    public void getClientIPCounts(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      Map<String, Long> statusCountMap = new HashMap<String, Long>();
      Row row = statusCodes.get(Bytes.toBytes("clientIPKey"));

      // Get the row using the row key
      if (row != null) {
      
        // Get the number of occurrences of each client IP address
        for (Map.Entry<byte[], byte[]> colValue : row.getColumns().entrySet()) {
          statusCountMap.put(Bytes.toString(colValue.getKey()), Bytes.toLong(colValue.getValue()));
        }
      }
      // Send response in JSON format
      responder.sendJson(statusCountMap);
    }

The new ``getClientIPCounts`` method that will query the Dataset (storage) for the IP address occurrences.

After you make your code changes to *ResponseCodeAnalyticsApp.java*, you can build the .JAR file by running::

	mvn clean package

Step 6: Redeploy and Restart
----------------------------
We now need to stop the existing Application. Bring up the Application's Overview (by clicking on the
*Overview* button in  the left sidebar, and selecting the Application's name from the list, 
or by clicking on the Application name *ResponseCodeAnalytics*, if it is in the top title bar,
if you are in an Element detail). Click the **Stop** buttons on the right side of the
*Process* and *Query* sections. This will stop the Flow and Procedure. You can tell by the
labels underneath the names of the Flow and Procedures.

Now, redeploy the Application. Return to the Reactor Overview (via the *Overview* button) and click the
*Load An App* button. Browse for the .JAR file (located in 
``/examples/ResponseCodeAnalytics/target``, and select it. The Application will be deployed.

Restart the Flow and Procedure by clicking on the Application name to return to the 
Application's overview, and click the *Start* buttons for both the ``LogAnalyticsFlow`` and the ``StatusCodeProcedure``.

Inject an event or two by following the practice described in `Step 3: Inject Data`_ to generate new entries with client IP
addresses in the DataStore.

Step 7: Checkout the Results
----------------------------
Click on the name of the Procedure (**StatusCodeProcedure**) to go to the Query view.
This time, use the method you added (``getClientIPCounts``) to find out the unique client IP addresses
and their counts::

	{"165.225.156.91":1}

Of course, if you have performed additional injections, your results will be different.
The total should match the number of injections you made after you restarted the application.


Where to Go Next
----------------
Now that you've had a look at Continuuity Reactor, take a look at:

- `Developer Examples <examples/index.html>`__,
  three different examples to run and experiment with.
