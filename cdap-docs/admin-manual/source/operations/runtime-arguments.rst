.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014-2015 Cask Data, Inc.

============================================
Runtime Arguments
============================================

.. include:: ../../../_common/_include/include-v260-deprecate-procedures.rst

Flows, Procedures, MapReduce programs, and Workflows can receive runtime arguments:

- For Flows and Procedures, runtime arguments are available to the ``initialize`` method in the context.

- For MapReduce, runtime arguments are available to the ``beforeSubmit`` and ``onFinish`` methods in the context.
  The ``beforeSubmit`` method can pass them to the Mappers and Reducers through the job configuration.

- When a Workflow receives runtime arguments, it passes them to each MapReduce in the Workflow.

.. highlight:: java

The ``initialize()`` method in this example accepts a runtime argument for the
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