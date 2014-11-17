/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.examples.wordcount;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Retrieve Counts service handler.
 */
public class RetrieveCountsHandler extends AbstractHttpServiceHandler {

  @UseDataSet("wordStats")
  private Table wordStatsTable;

  @UseDataSet("wordCounts")
  private KeyValueTable wordCountsTable;

  @UseDataSet("uniqueCount")
  private UniqueCountTable uniqueCountTable;

  @UseDataSet("wordAssocs")
  private AssociationTable associationTable;

  /**
   * Returns all from the global statistics.
   */
  @Path("stats")
  @GET
  public void getStats(HttpServiceRequest request, HttpServiceResponder responder) {
    long totalWords = 0L;
    long uniqueWords = 0L;
    double averageLength = 0.0;

    // Read the total_length and total_words to calculate average length
    Row result = wordStatsTable.get(new Get("totals", "total_length", "total_words"));
    if (!result.isEmpty()) {
      // Extract the total sum of lengths
      long totalLength = result.getLong("total_length", 0);

      // Extract the total count of words
      totalWords = result.getLong("total_words", 0);

      // Compute the average length
      if (totalLength != 0 && totalWords != 0) {
        averageLength = (double) totalLength / totalWords;

        // Read the unique word count
        uniqueWords = uniqueCountTable.readUniqueCount();
      }
    }

    // Return a map as JSON
    Map<String, Object> results = new HashMap<String, Object>();
    results.put("totalWords", totalWords);
    results.put("uniqueWords", uniqueWords);
    results.put("averageLength", averageLength);

    responder.sendJson(results);
  }

  @Path("count/{word}/{limit}")
  @GET
  public void getCount(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("word") String word, @PathParam("limit") int limit) {
    responder.sendJson(getCount(word, limit));
  }

  @Path("count/{word}")
  @GET
  public void getCount(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("word") String word) {
    responder.sendJson(getCount(word, 10));
  }

  @Path("assoc/{word1}/{word2}")
  @GET
  public void getAssoc(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("word1") String word1, @PathParam("word2") String word2) {
    // Read the top associated words
    long count = associationTable.getAssoc(word1, word2);

    // Return a map as JSON
    Map<String, Object> results = new HashMap<String, Object>();
    results.put("word1", word1);
    results.put("word2", word2);
    results.put("count", count);

    responder.sendJson(results);
  }

  private Map<String, Object> getCount(String word, int limit) {
    // Read the word count
    byte[] countBytes = wordCountsTable.read(Bytes.toBytes(word));
    long wordCount = countBytes == null ? 0L : Bytes.toLong(countBytes);

    // Read the top associated words
    Map<String, Long> wordsAssocs = associationTable.readWordAssocs(word, limit);

    // Build a map with results
    Map<String, Object> results = new HashMap<String, Object>();
    results.put("word", word);
    results.put("count", wordCount);
    results.put("assocs", wordsAssocs);

    return results;
  }
}
