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

package com.continuuity.examples.twitter;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.common.Bytes;

/**
 * A simple flow with a source flowlet that reads tweets.
 */
public class TwitterScanner implements Application {

  // DataSets
  public static final String TOP_USERS = "topUsers";
  public static final String TOP_HASH_TAGS = "topHashTags";
  public static final String WORD_COUNTS = "wordCounts";
  public static final String HASH_TAG_WORD_ASSOCS = "hashTagWordAssocs";
  // DataSet Constants
  public static final byte[] HASHTAG_SET = Bytes.toBytes("h");
  public static final byte[] USER_SET = Bytes.toBytes("u");
  public static final byte[] WORD_SET = Bytes.toBytes("w");
  // Twitter Constants
  // TODO: Remove this and change to runtime parameters
  public static final String[] USERNAMES = new String[]{
    "consumerintelli", "continuuity", "bigflowdev"
  };
  public static final String PASSWORD = "Wh00pa$$123!";

  public static void main(String[] args) {
    // Main method should be defined for Application to get deployed with Eclipse IDE plugin. DO NOT REMOVE THIS MAIN() METHOD.
  }

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("TwitterScanner")
      .setDescription("Example Twitter application")
      .noStream()
      .withDataSets()
        .add(new SortedCounterTable(TOP_USERS,
                                    new SortedCounterTable.SortedCounterConfig()))
        .add(new SortedCounterTable(TOP_HASH_TAGS,
                                    new SortedCounterTable.SortedCounterConfig()))
        .add(new CounterTable(WORD_COUNTS))
        .add(new CounterTable(HASH_TAG_WORD_ASSOCS))
      .withFlows()
        .add(new TwitterFlow())
      .withProcedures()
        .add(new TwitterProcedure())
      .noBatch()
      .build();
  }
}
