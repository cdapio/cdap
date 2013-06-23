/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.payvment.continuuity;


import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;
import com.payvment.continuuity.data.ActivityFeedTable;
import com.payvment.continuuity.data.CounterTable;
import com.payvment.continuuity.data.PopularFeed;
import com.payvment.continuuity.data.SortedCounterTable;
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
 * Input
 * <p>The input to this Flow is a stream named <i>social-actions</i> which
 * contains social action events in JSON format.  More information about the
 * schema of these events can be found in {@link SocialAction}.</p>
 * <p/>
 * Flowlets
 * <p>This Flow is made up of four Flowlets.
 * <p>The first flowlet, {@link SocialActionParserFlowlet}, is responsible
 * for parsing the social action JSON into the internal representation (as an
 * instance of a {@link SocialAction} passed through a {@link ProcessedSocialAction}).
 * <p>The second Flowlet, {@link SocialActionProcessorFlowlet}, performs the
 * primary processing and is responsible for the initial counter and score
 * updates to all of the necessary tables.  Counters are used to determine
 * the total score and hourly score for this product.
 * <p>Results from these operations, specifically the total and hourly scores,
 * are then passed on to the remaining two Flowlets,
 * {@link ActivityFeedUpdaterFlowlet} and {@link PopularFeedUpdaterFlowlet},
 * for final processing and insertion into the two respective feeds as
 * required.
 * <p>See the javadoc of each Flowlet class for more detailed information.
 * <p/>
 * Tables
 * <p>This Flow utilizes four Tables.
 * <p><i>productActions</i> is an instance of a {@link CounterTable} used to
 * track the number of times each social action type has occurred for every
 * individual product.  The primary key on this table is productId.
 * <p><i>allTimeScores</i> is another instance of a {@link CounterTable} used
 * to track the total score over all time of a given product.
 * <p><i>topScores</i> is an instance of a {@link SortedCounterTable} used to
 * track the scores of each product on an hourly basis and provides score
 * sorting capabilities to allow retrieving the top-n scored products for a
 * given hour and category.
 * <p><i>activityFeeds</i> is an instance of an ActivityFeedTable used
 * to store descending time-ordered feeds of products. <i>(NOTE: This is not
 * yet implemented as a separate table, coming soon)</i>
 * <p>See the javadoc of each table class for more detailed information.
 */
public class SocialActionFlow implements Flow {

  /**
   * Name of this Flow.
   */
  public static final String FLOW_NAME = "SocialActionProcessor";

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName(FLOW_NAME)
      .setDescription(FLOW_NAME)
      .withFlowlets()
        .add("action_parser", new SocialActionParserFlowlet())
        .add("action_processor", new SocialActionProcessorFlowlet())
        .add("activity_feed_updater", new ActivityFeedUpdaterFlowlet())
        .add("popular_feed_updater", new PopularFeedUpdaterFlowlet())
      .connect()
        .fromStream(LishApp.SOCIAL_ACTION_STREAM).to("action_parser")
        .from("action_parser").to("action_processor")
        .from("action_processor").to("activity_feed_updater")
        .from("action_processor").to("popular_feed_updater")
      .build();
  }

  /**
   *
   */
  public static class ProcessedSocialAction {
    public SocialAction socialAction;
    public Long scoreIncrease;
    public Long allTimeScore;

    public ProcessedSocialAction(SocialAction socialAction, Long scoreIncrease, Long allTimeScore) {
      this.socialAction = socialAction;
      this.scoreIncrease = scoreIncrease;
      this.allTimeScore = allTimeScore;
    }
  }

  /**
   *
   */
  public static class ProcessedSocialActionAndCountry extends ProcessedSocialAction {
    public String country;

    public ProcessedSocialActionAndCountry(ProcessedSocialAction processedAction, String country) {
      super(processedAction.socialAction, processedAction.scoreIncrease, processedAction.allTimeScore);
      this.country = country;
    }
  }

  /**
   * Flowlet that performs the primary updates to counters that track the
   * all time and hourly scores of products.
   */
  public static class SocialActionProcessorFlowlet extends AbstractFlowlet {

    public static int numProcessed = 0;
    int numErrors = 0;

    @UseDataSet(LishApp.PRODUCT_ACTION_TABLE)
    private CounterTable productActionCountTable;

    @UseDataSet(LishApp.ALL_TIME_SCORE_TABLE)
    private CounterTable allTimeScoreTable;

    @Output("activity")
    OutputEmitter<ProcessedSocialAction> activityUpdater;

    @Output("popular")
    OutputEmitter<ProcessedSocialActionAndCountry> popularUpdater;

    @ProcessInput
    public void process(SocialAction action) throws OperationException {

      // Determine score increase
      Long scoreIncrease = action.getSocialActionType().getScore();

      // Update product action count table
      this.productActionCountTable.incrementCounterSet(Bytes.toBytes(action.productId),
                                                       Bytes.toBytes(action.type),
                                                       1L);

      // Update all-time score
      Long allTimeScore = this.allTimeScoreTable.incrementSingleKey(Bytes.toBytes(action.productId), scoreIncrease);

      ProcessedSocialAction processedAction = new ProcessedSocialAction(action, scoreIncrease, allTimeScore);

      this.activityUpdater.emit(processedAction);

      // Emit a tuple for each country
      for (String country : action.country) {
        this.popularUpdater.emit(new ProcessedSocialActionAndCountry(processedAction, country));
      }
    }

  }

  /**
   * Flowlet that performs checks of values generated in the primary updates
   * and determines whether to insert an activity feed entry.  If it does,
   * then it will write an activity feed entry.
   */
  public static class ActivityFeedUpdaterFlowlet extends AbstractFlowlet {

    @UseDataSet(LishApp.ACTIVITY_FEED_TABLE)
    private ActivityFeedTable activityFeedTable;

    @ProcessInput("activity")
    public void process(ProcessedSocialAction processedAction) throws OperationException {

      // Doesn't do anything for now
      if (!shouldInsertFeedEntry(processedAction.scoreIncrease, processedAction.allTimeScore)) {
        return;
      }

      ActivityFeedEntry feedEntry = new ActivityFeedEntry(processedAction.socialAction.date,
                                                          processedAction.socialAction.storeId,
                                                          processedAction.socialAction.productId,
                                                          processedAction.allTimeScore);

      for (String country : processedAction.socialAction.country) {
        this.activityFeedTable.writeEntry(country, processedAction.socialAction.category, feedEntry);
      }
    }

    private static boolean shouldInsertFeedEntry(Long scoreIncrease, Long allTimeScore) {
      return true;
    }
  }

  /**
   * Flowlet that performs checks of values generated in the primary updates
   * and determines whether to perform any additional insertions for the
   * popular feed, and if so, performs those operations.
   */
  public static class PopularFeedUpdaterFlowlet extends AbstractFlowlet {

    @UseDataSet(LishApp.TOP_SCORE_TABLE)
    private SortedCounterTable topScoreTable;

    @ProcessInput("popular")
    public void process(ProcessedSocialActionAndCountry processedAction) throws OperationException {

      this.topScoreTable.increment(PopularFeed.makeRow(Helpers.hour(processedAction.socialAction.date),
                                                       processedAction.country,
                                                       processedAction.socialAction.category),
                                   Bytes.toBytes(processedAction.socialAction.productId), 1L);
    }
  }
}
