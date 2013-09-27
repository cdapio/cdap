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

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

import java.util.ArrayList;
import java.util.List;

/**
 * Twitter processor flowlet.
 */
public class TwitterProcessor extends AbstractFlowlet {

  @UseDataSet(TwitterScanner.TOP_HASH_TAGS)
  private SortedCounterTable topHashTags;
  @UseDataSet(TwitterScanner.TOP_USERS)
  private SortedCounterTable topUsers;
  @UseDataSet(TwitterScanner.TOP_USERS)
  private CounterTable wordCounts;
  @UseDataSet(TwitterScanner.HASH_TAG_WORD_ASSOCS)
  private CounterTable hashTagWordAssocs;

  @ProcessInput
  public void process(Tweet tweet) throws OperationException {
    String[] words = tweet.getText().split("\\s+");

    List<String> goodWords = new ArrayList<String>(words.length);
    for (String word : words) {
      if (word.length() < 3) {
        continue;
      }

      if (!word.equals(new String(word.getBytes()))) {
        continue;
      }

      goodWords.add(word.toLowerCase());
    }
    // Split tweet into individual words
    for (String word : goodWords) {
      if (word.startsWith("#")) {
        // Track top hash tags
        topHashTags.increment(
          TwitterScanner.HASHTAG_SET, Bytes.toBytes(word), 1L);
        // For every hash tag, track word associations
        for (String corWord : goodWords) {
          if (corWord.startsWith("#")) {
            continue;
          }

          hashTagWordAssocs.incrementCounterSet(word, corWord, 1L);
        }
      } else {
        // Track word counts
        wordCounts.incrementCounterSet(
          TwitterScanner.WORD_SET, Bytes.toBytes(word), 1L);
      }
    }

    // Track top users
    topUsers.increment(
      TwitterScanner.USER_SET, Bytes.toBytes(tweet.getUser()), 1L);
  }
}
