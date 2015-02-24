.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

============================================
Preferences
============================================

.. include:: ../../../_common/_include/include-v260-deprecate-procedures.rst

Preferences provide the ability to save configuration at various levels of the system including CDAP Instance,
Namespace, Application and Program. Configuration is represented by a map of string-string pairs. Preferences
can be retrieved/saved/deleted through REST API and/or through CLI. When programs are started, the Preferences
at different levels are collapsed into one map and it is accessible through ways described later. Preferences
are persisted across restart of Programs.

The overriding order of Preferences is:

CDAP Instance < Namespace < Application < Program < Runtime Arguments (passed in during start of the Program)

For example, if a configuration say, ``SAMPLE_KEY`` is set to 10 at Namespace level and at the Program level,
it is set to 5, then when the Program is started, the value set at Program level will override the value set at
Namespace level and thus will get 5 as the value for the key ``SAMPLE_KEY``.

Programs such as Flows, Procedures, MapReduce programs, Services, Workflows etc will receive the resolved preferences
and can be accessed through the ``getRuntimeArguments`` method of the context:

- For Flows and Procedures, preferences are available to the ``initialize`` method in the context.

- For MapReduce, preferences are available to the ``beforeSubmit`` and ``onFinish`` methods in the context.
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