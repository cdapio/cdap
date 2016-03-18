.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _preferences:

=================================
Preferences and Runtime Arguments
=================================

Preferences provide the ability to save configuration information at various levels of the system, including the
CDAP instance, namespace, application, and program levels. A configuration is represented by a map of string-string
pairs. Preferences can be retrieved, saved, and deleted through a RESTful API and through the Command Line Interface.
When programs are started, all the preferences at the different levels are collapsed into a single map. Preferences
are persisted across a restart of either programs or CDAP itself.

Order of Preferences
====================

The overriding order of preferences is (from lowest to highest precedence):

  CDAP Instance < Namespace < Application < Program < Runtime Arguments *(passed in during start of the program)*

**Example:** A configuration preference ``SAMPLE_KEY`` is set to 20 at the namespace level and is set to 10 at the
program level. When the program is started, the value set at the program level overrides the value set at
the namespace level and thus the value for the preference ``SAMPLE_KEY`` will be 10.

Programs such as flows, MapReduce programs, services, workflows and workers will receive the resolved preferences
and can be accessed through the ``getRuntimeArguments`` method of the context:

- For flows, services, and workers: preferences are available to the ``initialize`` method in the context.

- For MapReduce and Spark: preferences are available to the ``beforeSubmit`` and ``onFinish`` methods in the context.
  The ``beforeSubmit`` method can pass them to the mappers and reducers through the job configuration.

- When a workflow receives preferences, it passes them to each MapReduce in the workflow.

Preferences and Runtime Arguments in the CDAP UI
================================================
In the :ref:`CDAP UI <cdap-ui>`, preferences can be set at the different levels (as
described above), with runtime arguments being available at the lowest level of individual
elements.

Preferences are retained, and are persisted across different runs and restarts of
applications and CDAP. Runtime arguments (available under the *Arguments* buttons in the
CDAP UI) are one-time only, for a particular invocation, and are not persisted. Use the
*Preferences* button with the same key-value pairs for arguments you would like persisted.

Preferences Example
===================

.. highlight:: java

The ``initialize()`` method in this example accepts a configuration key ``disable.public`` for the
``TweetCollector`` flowlet. For example, we can disable using the Public Twitter Source
or provide Twitter API credentials::

  public class TweetCollector extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(TweetCollector.class);

    private OutputEmitter<Tweet> output;

    private CollectingThread collector;
    private BlockingQueue<Tweet> queue;

    private Metrics metrics;

    private TwitterStream twitterStream;

    @Override
    public void initialize(FlowletContext context) throws Exception {
      super.initialize(context);
      Map<String, String> args = context.getRuntimeArguments();

      if (args.containsKey("disable.public")) {
        LOG.info("Public Twitter source turned off");
        return;
      }

      if (!args.containsKey("oauth.consumerKey") || !args.containsKey("oauth.consumerSecret")
       || !args.containsKey("oauth.accessToken") || !args.containsKey("oauth.accessTokenSecret")) {
        final String CREDENTIALS_MISSING = "Twitter API credentials not provided in runtime arguments.";
        LOG.error(CREDENTIALS_MISSING);
  //      throw new IllegalArgumentException(CREDENTIALS_MISSING);
      }

      queue = new LinkedBlockingQueue<Tweet>(10000);
      collector = new CollectingThread();
      collector.start();
    }

System-populated Runtime Arguments
==================================

On each program run, CDAP populates the runtime arguments with pre-defined values (currently, one):

- ``logical.start.time``: the start time of the run as a timestamp in milliseconds.
  If the run was started by a schedule, this will be equal to the trigger time for the schedule.
  For example, if the schedule was set to run at midnight on Jan 1, 2016 UTC, the logical start time would be ``1451606400000``.

If an argument is passed in to start a program run, it will override the system-populated value.

Scoped Runtime Arguments for Workflow
=====================================
When a workflow is configured, you may want to pass specific runtime arguments to the different programs
and datasets used inside the workflow. To achieve this, you can prefix the runtime arguments with a ``<scope>``.
Currently supported scopes are ``dataset``, ``mapreduce``, and ``spark``.

**Example:** To set a runtime argument of ``read.timeout=30`` for the MapReduce program ``oneMapReduce`` in a workflow,
the argument can be provided with a scope of ``mapreduce.oneMapReduce.read.timeout=30``. In this case, ``oneMapReduce``
and the datasets used in ``oneMapReduce`` will receive two arguments: one with a scope of
``mapreduce.oneMapReduce.read.timeout=30``, and another with the scope extracted as ``read.timeout=30``.
Programs other than ``oneMapReduce`` and datasets used in them will receive only the single argument
``mapreduce.oneMapReduce.read.timeout=30``.

An argument can also be prefixed with ``<scope>.*`` to apply it to all programs or datasets in the workflow as
represented by the scope.

**Example:** If the runtime argument is specified as ``mapreduce.*.read.timeout=30``, all MapReduce programs and
datasets used in them will receive two arguments: one with the scope ``mapreduce.*.read.timeout=30`` and another
with the scope extracted as ``read.timeout=30``. Programs other than the MapReduce and the datasets used by them
will receive only the single argument ``mapreduce.*.read.timeout=30``.

Since the datasets can be used by programs running inside a workflow, a scope for them can be nested inside a
corresponding program scope.

**Example:** Consider the dataset ``myTable`` used by multiple programs in a workflow. The runtime argument
``cache.seconds=30`` for ``myTable`` can be provided as ``dataset.myTable.cache.seconds=30``. In this case,
the argument will be set for all programs that use ``myTable``. In order to set the argument only for the MapReduce
program ``myMR``, the dataset argument can be nested inside the program scope as
``mapreduce.myMR.dataset.myTable.cache.seconds=30``.

Examples of using Preferences
=============================
Other examples of using preferences are in these CDAP examples:

- :ref:`Data Cleansing: <examples-data-cleansing>` Uses a runtime argument with a MapReduce program
- :ref:`Wikipedia Pipeline: <examples-wikipedia-data-pipeline>` Uses runtime arguments
  with a stream, MapReduce program, and a Spark program
- :ref:`Sport Results: <examples-sport-results>` Uses a runtime argument with a MapReduce program
