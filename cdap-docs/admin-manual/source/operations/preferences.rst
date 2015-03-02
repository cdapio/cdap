.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

.. _preferences:

=================================
Preferences and Runtime Arguments
=================================

.. include:: ../../../_common/_include/include-v260-deprecate-procedures.rst

Preferences provide the ability to save configuration information at various levels of the system, including the
CDAP Instance, Namespace, Application, and Program levels. A configuration is represented by a map of string-string
pairs. Preferences can be retrieved, saved, and deleted through a RESTful API and through the Command Line Interface.
When programs are started, all the Preferences at the different levels are collapsed into a single map. Preferences
are persisted across a restart of Programs and CDAP.

The overriding order of Preferences is:

CDAP Instance < Namespace < Application < Program < Runtime Arguments (passed in during start of the Program)

Example: A configuration preference ``SAMPLE_KEY`` is set to 20 at the Namespace level and is set to 10 at the
Program level. When the Program is started, the value set at the Program level overrides the value set at
the Namespace level and thus the value for the preference ``SAMPLE_KEY`` will be 10.

Programs such as Flows, MapReduce programs, Services, Workflows and Workers will receive the resolved preferences
and can be accessed through the ``getRuntimeArguments`` method of the context:

- For Flows, Services, Workers preferences are available to the ``initialize`` method in the context.

- For MapReduce, Spark preferences are available to the ``beforeSubmit`` and ``onFinish`` methods in the context.
  The ``beforeSubmit`` method can pass them to the Mappers and Reducers through the job configuration.

- When a Workflow receives preferences, it passes them to each MapReduce in the Workflow.

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
        String publicArg = args.get("disable.public");
        LOG.info("Public Twitter source turned off (disable.public={})", publicArg);
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