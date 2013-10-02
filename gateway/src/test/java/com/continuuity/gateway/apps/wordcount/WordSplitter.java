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

package com.continuuity.gateway.apps.wordcount;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Word splitter Flowlet.
 */
public class WordSplitter extends AbstractFlowlet {

  private static final byte[] TOTALS_ROW = Bytes.toBytes("totals");
  private static final byte[] TOTAL_LENGTH = Bytes.toBytes("total_length");
  private static final byte[] TOTAL_WORDS = Bytes.toBytes("total_words");
  @UseDataSet("wordStats")
  private Table wordStatsTable;
  @Output("wordOut")
  private OutputEmitter<String> wordOutput;
  @Output("wordArrayOut")
  private OutputEmitter<List<String>> wordListOutput;

  @ProcessInput
  public void process(StreamEvent event) {
    // Input is a String, need to split it by whitespace
    String inputString = Charset.forName("UTF-8")
      .decode(event.getBody()).toString();

    String[] words = inputString.split("\\s+");
    List<String> wordList = new ArrayList<String>(words.length);

    long sumOfLengths = 0;
    long wordCount = 0;

    // We have an array of words, now remove all non-alpha characters
    for (String word : words) {
      word = word.replaceAll("[^A-Za-z]", "");
      if (!word.isEmpty()) {
        // emit every word that remains
        wordOutput.emit(word);
        wordList.add(word);
        sumOfLengths += word.length();
        wordCount++;
      }
    }

    // Count other word statistics (word length, total words seen)
    this.wordStatsTable.increment(TOTALS_ROW,
                                  new byte[][]{TOTAL_LENGTH, TOTAL_WORDS},
                                  new long[]{sumOfLengths, wordCount});

    // Send the list of words to the associater
    wordListOutput.emit(wordList);

  }
}
