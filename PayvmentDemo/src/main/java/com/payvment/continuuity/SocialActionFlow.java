package com.payvment.continuuity;


import com.continuuity.api.data.Increment;
import com.continuuity.api.data.Write;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.StreamsConfigurator;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;
import com.payvment.continuuity.data.ActivityFeed;
import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;
import com.payvment.continuuity.entity.SocialAction;
import com.payvment.continuuity.lib.CounterTable;
import com.payvment.continuuity.lib.SortedCounterTable;
import com.payvment.continuuity.util.Bytes;
import com.payvment.continuuity.util.Constants;
import com.payvment.continuuity.util.Helpers;

/**
 * Flow application used to process Lish social actions in order to generate
 * activity feeds and popular feeds of products.
 * <p>
 * This currently generates activity and popular feeds on a
 * per-product-category basis.
 * <p>
 * <b>Flow Design</b>
 * <p>
 *   <u>Input</u>
 *   <p>The input to this Flow is a stream named <i>social-actions</i> which
 *   contains social action events in JSON format.  More information about the
 *   schema of these events can be found in {@link SocialAction}.</p>
 * <p>
 *   <u>Flowlets</u>
 *   <p>This Flow is made up of four Flowlets.
 *   <p>The first flowlet, {@link SocialActionParserFlowlet}, is responsible
 *   for parsing the social action JSON into the internal representation (as an
 *   instance of a {@link SocialAction} passed through a {@link Tuple}).
 *   <p>The second Flowlet, {@link SocialActionProcessorFlowlet}, performs the
 *   primary processing and is responsible for the initial counter and score
 *   updates to all of the necessary tables.  Counters are used to determine
 *   the total score and hourly score for this product.
 *   <p>Results from these operations, specifically the total and hourly scores,
 *    are then passed on to the remaining two Flowlets,
 *    {@link ActivityFeedUpdaterFlowlet} and {@link PopularFeedUpdaterFlowlet},
 *    for final processing and insertion into the two respective feeds as
 *    required.
 *   <p>See the javadoc of each Flowlet class for more detailed information.
 * <p>
 *   <u>Tables</u>
 *   <p>This Flow utilizes four Tables.
 *   <p><i>productActions</i> is an instance of a {@link CounterTable} used to
 *   track the number of times each social action type has occurred for every
 *   individual product.  The primary key on this table is product_id.
 *   <p><i>allTimeScores</i> is another instance of a {@link CounterTable} used
 *   to track the total score over all time of a given product.
 *   <p><i>topScores</i> is an instance of a {@link SortedCounterTable} used to
 *   track the scores of each product on an hourly basis and provides score
 *   sorting capabilities to allow retrieving the top-n scored products for a
 *   given hour and category.
 *   <p><i>activityFeeds</i> is an instance of an {@link ActivityFeedTable} used
 *   to store descending time-ordered feeds of products. <i>(NOTE: This is not
 *   yet implemented as a separate table)</i>
 *   <p>See the javadoc of each table class for more detailed information.
 */
public class SocialActionFlow implements Flow {

  /**
   * Name of the input stream carrying JSON formatted Lish social actions.
   */
  public static final String inputStream = "social-actions";

  /**
   * Name of this Flow.
   */
  public static final String flowName = "SocialActionProcessor";

  @Override
  public void configure(FlowSpecifier specifier) {

    // Set metadata fields
    specifier.name(flowName);
    specifier.email("dev@continuuity.com");
    specifier.application("Cluster Activity Feeds");
    specifier.company("Continuuity+Payvment");

    // Declare all of the flowlets within the flow
    specifier.flowlet("action_parser", SocialActionParserFlowlet.class,
        1);
    specifier.flowlet("action_processor", SocialActionProcessorFlowlet.class,
        1);
    specifier.flowlet("activity_feed_updater", ActivityFeedUpdaterFlowlet.class,
        1);
    specifier.flowlet("popular_feed_updater", PopularFeedUpdaterFlowlet.class,
        1);

    // Define input stream and connect to parsing flowlet
    specifier.stream(inputStream);
    specifier.input(inputStream, "action_parser");

    // Wire up the remaining flowlet connections
    // (parser to processor, processor to updaters)
    specifier.connection("action_parser", "action_processor");
    specifier.connection("action_processor", "activity_feed_updater");
    specifier.connection("action_processor", "popular_feed_updater");
  }

  /**
   * Tuple schema used between {@link SocialActionParserFlowlet} and
   * {@link SocialActionProcessorFlowlet}.
   * <p>
   * Schema contains only a {@link SocialAction} object.
   */
  public static final TupleSchema SOCIAL_ACTION_TUPLE_SCHEMA =
      new TupleSchemaBuilder().add("action", SocialAction.class).create();

  /**
   * Tuple schema used between {@link SocialActionProcessorFlowlet} and the two
   * connecting Flowlets, {@link ActivityFeedUpdaterFlowlet} and
   * {@link PopularFeedUpdaterFlowlet}.
   * <p>
   * Schema contains a {@link SocialAction} object, the score increase for this
   * event, and Long values derived from the results of counter increment
   * operations performed by the processor.  Specifically, these values are the
   * all-time total score and the hourly score of the product in the social
   * action. 
   */
  public static final TupleSchema PROCESSED_ACTION_TUPLE_SCHEMA =
      new TupleSchemaBuilder()
          .add("action", SocialAction.class)
          .add("score-increase", Long.class)
          .add("all-time-score", Long.class)
          .add("hourly-score", Long.class)
          .create();

  /**
   * Flowlet that performs the primary updates to counters that track the
   * all time and hourly scores of products.
   */
  public static class SocialActionProcessorFlowlet extends ComputeFlowlet {

    static int numProcessed = 0;

    static int numErrors = 0;

    @Override
    public void configure(StreamsConfigurator configurator) {
      // Input schema is a social action
      configurator.getDefaultTupleInputStream()
          .setSchema(SOCIAL_ACTION_TUPLE_SCHEMA);
      // Output schema contains action, category, score, Increment pass-thrus
      configurator.getDefaultTupleOutputStream()
          .setSchema(PROCESSED_ACTION_TUPLE_SCHEMA);
    }

    private CounterTable productActionCountTable;

    private CounterTable allTimeScoreTable;

    private SortedCounterTable topScoreTable;

    @Override
    public void initialize() {
      this.productActionCountTable = new CounterTable("productActions",
          getFlowletContext().getDataFabric(), getFlowletContext());
      this.allTimeScoreTable = new CounterTable("allTimeScores",
          getFlowletContext().getDataFabric(), getFlowletContext());
      this.topScoreTable = new SortedCounterTable("topScores",
          getFlowletContext().getDataFabric(), getFlowletContext(),
          new SortedCounterTable.SortedCounterConfig());
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      SocialAction action = tuple.get("action");
      TupleBuilder tupleBuilder = new TupleBuilder();
      tupleBuilder.set("action", action);
      
      // Update product action count table async
      this.productActionCountTable.incrementCounterSet(
          Bytes.toBytes(action.product_id), Bytes.toBytes(action.type), 1L);
      
      // Determine score increase
      Long scoreIncrease = action.getSocialActionType().getScore();
      tupleBuilder.set("score-increase", scoreIncrease);
      
      // Update all-time score, but put increment into tuple for pass-thru
      Increment allTimeScore = allTimeScoreTable.generateSingleKeyIncrement(
          Bytes.add(Constants.PRODUCT_ALL_TIME_PREFIX,
              Bytes.toBytes(action.product_id)), scoreIncrease);
      tupleBuilder.set("all-time-score", allTimeScore);
      
      // Update time bucketed top-score table, also put increment into tuple
      Increment topScoreHourly = topScoreTable.generatePrimaryCounterIncrement(
          Bytes.add(Bytes.toBytes(Helpers.hour(action.date)),
              Bytes.toBytes(action.category)),
          Bytes.toBytes(action.product_id), scoreIncrease);
      tupleBuilder.set("hourly-score", topScoreHourly);
      
      collector.add(tupleBuilder.create());
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }
  }

  public static class ActivityFeedUpdaterFlowlet extends ComputeFlowlet {

    static int numProcessed = 0;

    @Override
    public void configure(StreamsConfigurator configurator) {
      // Input schema contains action and Increment pass-thrus
      configurator.getDefaultTupleInputStream()
          .setSchema(PROCESSED_ACTION_TUPLE_SCHEMA);
      // No output
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      Long scoreIncrease = tuple.get("score-increase");
      Long allTimeScore = tuple.get("all-time-score");
      Long hourlyScore = tuple.get("hourly-score");
      if (!shouldInsertFeedEntry(scoreIncrease, allTimeScore, hourlyScore)) {
        return;
      }
      // Insert feed entry
      SocialAction action = tuple.get("action");
      ActivityFeedEntry feedEntry = new ActivityFeedEntry(action.date,
          action.store_id, action.product_id, allTimeScore);
      Write feedEntryWrite = new Write(Constants.ACTIVITY_FEED_TABLE,
          ActivityFeed.makeActivityFeedRow(action.category),
          feedEntry.getColumn(), feedEntry.getValue());
      collector.add(feedEntryWrite);
    }
    
    private static boolean shouldInsertFeedEntry(Long scoreIncrease,
        Long allTimeScore, Long hourlyScore) {
      return true;
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }
  }

  public static class PopularFeedUpdaterFlowlet extends ComputeFlowlet {

    static int numProcessed = 0;

    @Override
    public void configure(StreamsConfigurator configurator) {
      // Input schema contains action and Increment pass-thrus
      configurator.getDefaultTupleInputStream()
          .setSchema(PROCESSED_ACTION_TUPLE_SCHEMA);
      // No output
    }

    private SortedCounterTable topScoreTable;

    @Override
    public void initialize() {
      this.topScoreTable = new SortedCounterTable("topScores",
          getFlowletContext().getDataFabric(), getFlowletContext(),
          new SortedCounterTable.SortedCounterConfig());
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      Long scoreIncrease = tuple.get("score-increase");
      Long hourlyScore = tuple.get("hourly-score");
      SocialAction action = tuple.get("action");
      // Let top score perform any additional indexing increments
      this.topScoreTable.performSecondaryCounterIncrements(
          Bytes.add(Bytes.toBytes(Helpers.hour(action.date)),
              Bytes.toBytes(action.category)),
          Bytes.toBytes(action.product_id), scoreIncrease, hourlyScore);
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }
  }
}
