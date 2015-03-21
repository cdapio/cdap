.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _preferences:

=================================
Preferences and Runtime Arguments
=================================

Preferences provide the ability to save configuration information at various levels of the system, including the
CDAP Instance, Namespace, Application, and Program levels. A configuration is represented by a map of string-string
pairs. Preferences can be retrieved, saved, and deleted through a RESTful API and through the Command Line Interface.
When programs are started, all the Preferences at the different levels are collapsed into a single map. Preferences
are persisted across a restart of either Programs or CDAP itself.

Order of Preferences
====================

The overriding order of Preferences is (from lowest to highest precedence):

CDAP Instance < Namespace < Application < Program < Runtime Arguments (passed in during start of the Program)

Example: A configuration preference ``SAMPLE_KEY`` is set to 20 at the Namespace level and is set to 10 at the
Program level. When the Program is started, the value set at the Program level overrides the value set at
the Namespace level and thus the value for the preference ``SAMPLE_KEY`` will be 10.

Programs such as Flows, MapReduce programs, Services, Workflows and Workers will receive the resolved preferences
and can be accessed through the ``getRuntimeArguments`` method of the context:

- For Flows, Services, and Workers: preferences are available to the ``initialize`` method in the context.

- For MapReduce and Spark: preferences are available to the ``beforeSubmit`` and ``onFinish`` methods in the context.
  The ``beforeSubmit`` method can pass them to the Mappers and Reducers through the job configuration.

- When a Workflow receives preferences, it passes them to each MapReduce in the Workflow.

Preferences Example
===================

.. highlight:: java

The ``initialize()`` method in this example accepts a configuration key ``disable.public`` for the
``TweetCollector`` Flowlet. For example, we can disable using the Public Twitter Source
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

Scoped Runtime Arguments for Workflow
=====================================
When a Workflow is configured, you may want to pass specific runtime arguments to the different programs
and datasets used inside the Workflow. To achieve this, you can prefix the runtime arguments with a ``<scope>``.
Currently supported scopes are ``dataset``, ``mapreduce``, and ``spark``.

Example: To set a runtime argument of ``read.timeout=30`` for the MapReduce program ``oneMapReduce`` in a Workflow,
the argument can be provided with a scope of ``mapreduce.oneMapReduce.read.timeout=30``. In this case, ``oneMapReduce``
and the datasets used in ``oneMapReduce`` will receive two arguments: one with a scope of
``mapreduce.oneMapReduce.read.timeout=30``, and another with the scope extracted as ``read.timeout=30``.
Programs other than ``oneMapReduce`` and datasets used in them will receive only the single argument
``mapreduce.oneMapReduce.read.timeout=30``.

An argument can also be prefixed with ``<scope>.*`` to apply it to all programs or datasets in the Workflow as
represented by the scope.

Example: If the runtime argument is specified as ``mapreduce.*.read.timeout=30``, all MapReduce programs and
datasets used in them will receive two arguments: one with the scope ``mapreduce.*.read.timeout=30`` and another
with the scope extracted as ``read.timeout=30``. Programs other than the MapReduce and the datasets used by them
will receive only the single argument ``mapreduce.*.read.timeout=30``.

Since the datasets can be used by programs running inside a Workflow, a scope for them can be nested inside a
corresponding program scope.

Example: Consider the dataset ``myTable`` used by multiple programs in a Workflow. The runtime argument
``cache.seconds=30`` for ``myTable`` can be provided as ``dataset.myTable.cache.seconds=30``. In this case,
the argument will be set for all programs that use ``myTable``. In order to set the argument only for the MapReduce
program ``myMR``, the dataset argument can be nested inside the program scope as ``mapreduce.myMR.dataset.myTable.cache.seconds=30``.
