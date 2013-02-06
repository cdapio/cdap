package com.payvment.continuuity;


import com.continuuity.api.data.Increment;
import com.continuuity.api.data.lib.CounterTable;
import com.continuuity.api.data.lib.SortedCounterTable;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.util.Helpers;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FailureHandlingPolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;
import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;
import com.payvment.continuuity.data.ActivityFeedTable;
import com.payvment.continuuity.data.PopularFeed;
import com.payvment.continuuity.entity.SocialAction;

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
 *   <p><i>activityFeeds</i> is an instance of an ActivityFeedTable used
 *   to store descending time-ordered feeds of products. <i>(NOTE: This is not
 *   yet implemented as a separate table, coming soon)</i>
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
    specifier.application("ClusterFeeds");
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
    specifier.connection("action_processor", "activity",
        "activity_feed_updater", "in");
    specifier.connection("action_processor", "popular",
        "popular_feed_updater", "in");
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
   * Tuple schema used between {@link SocialActionProcessorFlowlet} and the
   * activity feed Flowlet, {@link ActivityFeedUpdaterFlowlet}.
   * <p>
   * Schema contains a {@link SocialAction} object, the score increase for this
   * event, the country, and the Long value derived from the result of the
   * counter increment operation performed by the processor.  Specifically, this
   * value is the all-time score of the product in the social action. 
   */
  public static final TupleSchema PROCESSED_ACTION_ACTIVITY_TUPLE_SCHEMA =
      new TupleSchemaBuilder()
          .add("action", SocialAction.class)
          .add("score-increase", Long.class)
          .add("all-time-score", Long.class)
          .create();

  /**
   * Tuple schema used between {@link SocialActionProcessorFlowlet} and the
   * popular feed Flowlet, {@link PopularFeedUpdaterFlowlet}.
   * <p>
   * Schema contains a {@link SocialAction} object, the score increase for this
   * event, the country, and the Long value derived from the result of the
   * counter increment operation performed by the processor.  Specifically, this
   * value is the hourly score of the product in the social action for the
   * category and country. 
   */
  public static final TupleSchema PROCESSED_ACTION_POPULAR_TUPLE_SCHEMA =
      new TupleSchemaBuilder()
          .add("action", SocialAction.class)
          .add("score-increase", Long.class)
          .add("country", String.class)
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
    public void configure(FlowletSpecifier configurator) {
      // Input schema is a social action
      configurator.getDefaultFlowletInput()
          .setSchema(SOCIAL_ACTION_TUPLE_SCHEMA);
      // Output schemas contains action, country, score, Increment pass-thrus
      configurator.addFlowletOutput("activity")
          .setSchema(PROCESSED_ACTION_ACTIVITY_TUPLE_SCHEMA);
      configurator.addFlowletOutput("popular")
          .setSchema(PROCESSED_ACTION_POPULAR_TUPLE_SCHEMA);
    }

    private CounterTable productActionCountTable;

    private CounterTable allTimeScoreTable;

    private SortedCounterTable topScoreTable;

    @Override
    public void initialize() {
      this.productActionCountTable = new CounterTable("productActions");
      getFlowletContext().getDataSetRegistry().registerDataSet(
          this.productActionCountTable);
      this.allTimeScoreTable = new CounterTable("allTimeScores");
      getFlowletContext().getDataSetRegistry().registerDataSet(
          this.allTimeScoreTable);
      this.topScoreTable = new SortedCounterTable("topScores",
          new SortedCounterTable.SortedCounterConfig());
      getFlowletContext().getDataSetRegistry().registerDataSet(
          this.topScoreTable);
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      SocialAction action = tuple.get("action");
      
      // Determine score increase
      Long scoreIncrease = 0L;
      try {
        scoreIncrease = action.getSocialActionType().getScore();
      } catch (IllegalArgumentException iae) {
        getFlowletContext().getLogger().error(
            "SocialActionProcessor Flowet received unknown action type: " +
                action.type);
        return;
      }
      
      // Update product action count table async
      this.productActionCountTable.incrementCounterSet(
          Bytes.toBytes(action.product_id), Bytes.toBytes(action.type), 1L);
      
      // Update all-time score, but put increment into tuple for pass-thru
      Increment allTimeScore = allTimeScoreTable.generateSingleKeyIncrement(
              Bytes.toBytes(action.product_id), scoreIncrease);
      
      // Create and emit activity tuple
      TupleBuilder tupleBuilder = new TupleBuilder();
      tupleBuilder.set("action", action);
      tupleBuilder.set("score-increase", scoreIncrease);
      tupleBuilder.set("all-time-score", allTimeScore);
      collector.add("activity", tupleBuilder.create());
      
      // For each country, update time bucketed top-score table, put increment
      // into tuple and emit a tuple for each country
      for (String country : action.country) {
        Increment topScoreHourly =
            topScoreTable.generatePrimaryCounterIncrement(
                PopularFeed.makeRow(Helpers.hour(action.date),
                    country, action.category),
          Bytes.toBytes(action.product_id), scoreIncrease);
        tupleBuilder = new TupleBuilder();
        tupleBuilder.set("action", action);
        tupleBuilder.set("score-increase", scoreIncrease);
        tupleBuilder.set("country", country);
        tupleBuilder.set("hourly-score", topScoreHourly);
        collector.add("popular", tupleBuilder.create());
      }
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }

    @Override
    public FailureHandlingPolicy onFailure(Tuple tuple, TupleContext context,
        FailureReason reason) {
      getFlowletContext().getLogger().error(
          "SocialActionProcessor Flowet Processing Failed : " +
              reason.toString() + ", retrying");
      return FailureHandlingPolicy.RETRY;
    }
  }

  /**
   * Flowlet that performs checks of values generated in the primary updates
   * and determines whether to insert an activity feed entry.  If it does,
   * then it will write an activity feed entry.
   */
  public static class ActivityFeedUpdaterFlowlet extends ComputeFlowlet {

    static int numProcessed = 0;

    @Override
    public void configure(FlowletSpecifier configurator) {
      // Input schema contains action and Increment pass-thrus
      configurator.getDefaultFlowletInput()
          .setSchema(PROCESSED_ACTION_ACTIVITY_TUPLE_SCHEMA);
      // No output
    }

    private ActivityFeedTable activityFeedTable;

    @Override
    public void initialize() {
      this.activityFeedTable = new ActivityFeedTable();
      getFlowletContext().getDataSetRegistry().registerDataSet(
          this.activityFeedTable);
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      Long scoreIncrease = tuple.get("score-increase");
      Long allTimeScore = tuple.get("all-time-score");
      if (!shouldInsertFeedEntry(scoreIncrease, allTimeScore)) {
        return;
      }
      // Insert feed entry
      SocialAction action = tuple.get("action");
      ActivityFeedEntry feedEntry = new ActivityFeedEntry(action.date,
          action.store_id, action.product_id, allTimeScore);
      for (String country : action.country) {
        activityFeedTable.writeEntry(country, action.category, feedEntry);
      }
    }
    
    private static boolean shouldInsertFeedEntry(Long scoreIncrease,
        Long allTimeScore) {
      return true;
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }

    @Override
    public FailureHandlingPolicy onFailure(Tuple tuple, TupleContext context,
        FailureReason reason) {
      getFlowletContext().getLogger().error(
          "ActivityFeedUpdater Flowet Processing Failed : " +
              reason.toString() + ", retrying");
      return FailureHandlingPolicy.RETRY;
    }
  }

  /**
   * Flowlet that performs checks of values generated in the primary updates
   * and determines whether to perform any additional insertions for the
   * popular feed, and if so, performs those operations.
   */
  public static class PopularFeedUpdaterFlowlet extends ComputeFlowlet {

    static int numProcessed = 0;

    @Override
    public void configure(FlowletSpecifier configurator) {
      // Input schema contains action and Increment pass-thrus
      configurator.getDefaultFlowletInput()
          .setSchema(PROCESSED_ACTION_POPULAR_TUPLE_SCHEMA);
      // No output
    }

    private SortedCounterTable topScoreTable;

    @Override
    public void initialize() {
      this.topScoreTable = new SortedCounterTable("topScores",
          new SortedCounterTable.SortedCounterConfig());
      getFlowletContext().getDataSetRegistry().registerDataSet(
          this.topScoreTable);
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      Long scoreIncrease = tuple.get("score-increase");
      Long hourlyScore = tuple.get("hourly-score");
      SocialAction action = tuple.get("action");
      String country = tuple.get("country");
      // Let top score perform any additional indexing increments
      this.topScoreTable.performSecondaryCounterIncrements(
          PopularFeed.makeRow(Helpers.hour(action.date), country,
              action.category),
          Bytes.toBytes(action.product_id), scoreIncrease, hourlyScore);
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }

    @Override
    public FailureHandlingPolicy onFailure(Tuple tuple, TupleContext context,
        FailureReason reason) {
      getFlowletContext().getLogger().error(
          "PopularFeedUpdater Flowet Processing Failed : " +
              reason.toString() + ", retrying");
      return FailureHandlingPolicy.RETRY;
    }
  }
}
