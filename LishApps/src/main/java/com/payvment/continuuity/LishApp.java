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

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.stream.Stream;
import com.payvment.continuuity.data.ActivityFeedTable;
import com.payvment.continuuity.data.ClusterTable;
import com.payvment.continuuity.data.CounterTable;
import com.payvment.continuuity.data.SortedCounterTable;

/**
 * Payvment Lish Feeds on the Continuuity reactor
 */
public class LishApp implements Application {

  public static final String APP_NAME = "LishApp";
  public static final String APP_DESC = "Continuuity+Payvment Lish Application";

  /**
   * Sent through clusters stream to trigger a reset.
   */
  public static final String CLUSTER_RESET_FLAG = "reset_clusters";

  /* DataSet names */

  public static final String ACTIVITY_FEED_TABLE = "activity_feed_table";
  public static final String CLUSTER_TABLE = "cluster_table";
  public static final String COUNTER_TABLE = "counter_table";
  public static final String SORTED_COUNTER_TABLE = "sorted_counter_table";
  public static final String PRODUCT_ACTION_TABLE = "productActions";
  public static final String ALL_TIME_SCORE_TABLE = "allTimeScores";
  public static final String TOP_SCORE_TABLE = "topScores";

  /* Stream names */

  /**
   * Name of the input stream carrying JSON formatted Lish social actions.
   */
  public static final String SOCIAL_ACTION_STREAM = "social-actions";

  /**
   * Name of the input stream carrying CSV Payvment generated clusters.
   */
  public static final String CLUSTER_STREAM = "clusters";

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
        .setName(APP_NAME)
        .setDescription(APP_DESC)
        .withStreams()
          .add(new Stream(SOCIAL_ACTION_STREAM))
          .add(new Stream(CLUSTER_STREAM))
        .withDataSets()
          .add(new ActivityFeedTable(ACTIVITY_FEED_TABLE))
          .add(new ClusterTable(CLUSTER_TABLE))
          .add(new CounterTable(COUNTER_TABLE))
          .add(new SortedCounterTable(SORTED_COUNTER_TABLE))
          .add(new CounterTable(PRODUCT_ACTION_TABLE))
          .add(new CounterTable(ALL_TIME_SCORE_TABLE))
          .add(new SortedCounterTable(TOP_SCORE_TABLE))
        .withFlows()
          .add(new SocialActionFlow())
          .add(new ClusterWriterFlow())
        .withProcedures()
          .add(new ClusterFeedQueryProvider())
        .noBatch()
        .build();
  }
}
