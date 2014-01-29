.. :Author: John Jackson
   :Description: Operating a Continuuity Reactor

===================================
Operating a Continuuity Reactor
===================================

------------------------------
Building Big Data Applications
------------------------------

.. reST Editor: section-numbering::

.. reST Editor: contents::


Putting Continuuity Reactor into Production
===========================================

The Continuuity Reactor can be run in different modes: in-memory mode for unit testing, Local Reactor for local testing, and Hosted or Enterprise Reactor for staging and production. In addition, you have the option to get a free Sandbox Reactor in the cloud.

Regardless of the runtime edition, the Reactor is fully functional and the code you develop never changes. However performance and scale are limited when using in-memory, local mode or Sandbox Reactors.

In-memory Continuuity Reactor
-----------------------------
The in-memory Continuuity Reactor allows you to easily run the Reactor for use in unit tests. In this mode, the underlying Big Data infrastructure is emulated using in-memory data structures and there is no persistence. The Dashboard is not available in this mode.

[DOCNOTE: FIXME!]: is this still available?

Local Continuuity Reactor
-------------------------

The Local Continuuity Reactor allows you to run the entire Reactor stack in a single JVM on your local machine and also includes a local version of the Reactor Dashboard. The underlying Big Data infrastructure is emulated on top of your local file system. All data is persisted.

See the *Continuuity Reactor Getting Started Guide* included in 
the *Continuuity Reactor SDK* for information on how to start and manage your Local Reactor.


Sandbox Continuuity Reactor
---------------------------
The Sandbox Continuuity Reactor is a free version of the Reactor that is hosted and operated in the cloud. However, it does not provide the same scalability and performance as either the Hosted Reactor or the Enterprise Reactor. The Sandbox Reactor is an excellent way to evaluate all of the features of the “push-to-cloud” functionality of a Hosted Reactor or Enterprise Reactor without charge.

To self-provision a free Sandbox Reactor, login to your account at the 
`Account Home page <https://accounts.continuuity.com>`_. On the *Overview* tab, you can start the provisioning of a Sandbox Continuuity Reactor. 

To use the Reactor, you'll need an API key, which can be found (and generated) on the *Profile* tab of your account.

Hosted and Enterprise Continuuity Reactors
------------------------------------------

The Hosted Continuuity Reactor and the Enterprise Continuuity Reactor both run in fully distributed mode. This includes distributed and highly available deployments of the underlying Hadoop infrastructure in addition to the other system components of the Reactor. Production applications should always be run on either a Hosted Reactor or an Enterprise Reactor.

To learn more about getting your own Hosted Reactor or Enterprise Reactor, see
`Continuuity Products <http://www.continuuity.com/products>`_.

Runtime Arguments
-----------------

Flows, Procedures, MapReduce and Workflows can receive runtime arguments:

- For Flows and Procedures, runtime arguments are available to the initialize method in the context.

- For MapReduce, runtime arguments are available to the ``beforeSubmit`` and ``onFinish`` methods in the context. The ``beforeSubmit`` method can pass them to the mappers and reducers through the job configuration.

- When a Workflow receives runtime arguments it passes them to each MapReduce in the Workflow.

The ``initialize()`` method in this example accepts a runtime argument for the HelloWorld Procedure. For example, we can change the greeting from “Hello”, to “Good Morning” by passing a runtime argument::

	public static class Greeting extends AbstractProcedure {
	
	  @UseDataSet("whom")
	  KeyValueTable whom;
	  private String greeting;
	
	  public void initialize(ProcedureContext context) {
	    Map<String, String> args = context.getRuntimeArguments();
	    greeting = args.get("greeting");
	    if (greeting == null) {
	      greeting = "Hello";
	    }
	  }
	
	  @Handle("greet")
	  public void greet(ProcedureRequest request,
	                    ProcedureResponder responder) throws Exception {
	    byte[] name = whom.read(NameSaver.NAME);
	    String toGreet = name != null ? new String(name) : "World";
	    responder.sendJson(greeting + " " + toGreet + "!");
	  }
	}

Scaling Instances
------------------

Logging
=======

The Reactor supports logging through standard
`SLF4J (Simple Logging Facade for Java) <http://www.slf4j.org/manual.html>`_ APIs. 
For instance, in a Flowlet you can write::

	private static Logger LOG = LoggerFactory.getLogger(WordCounter.class);
	...
	@ProcessInput
	public void process(String line) {
	  LOG.info(this.getContext().getName() + ": Received line " + line);
	  ... // processing
	  LOG.info(this.getContext().getName() + ": Emitting count " + wordCount);
	  output.emit(wordCount);
	}

The log messages emitted by your Application code can be viewed in two different ways:

- All log messages of an Application can be viewed in the Continuuity Reactor Dashboard
  by clicking the *Logs* button in the Flow or Procedure screens.
- Using the `Continuuity Reactor HTTP REST interface <rest.html>`_ .


System Logs
-----------

Application Logs
----------------

Log Explorer
------------

Metrics
=======

System Metrics
--------------

Flow/Flowlets
.............

DataSet
.......

Event Rate, Query Rate, Storage Rate
....................................

User-defined Metrics
--------------------

Metrics Explorer
----------------

Resource Views
==============


.. include:: includes/footer.rst
