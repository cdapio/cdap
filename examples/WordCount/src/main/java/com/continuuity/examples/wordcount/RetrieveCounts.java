/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.wordcount;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.lib.KeyValueTable;
import com.continuuity.api.dataset.table.Get;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.api.procedure.ProcedureResponse.Code;

import java.util.Map;
import java.util.TreeMap;

/**
 * Retrieve count Procedure.
 */
public class RetrieveCounts extends AbstractProcedure {

  @UseDataSet("wordStats")
  private Table wordStatsTable;
  @UseDataSet("wordCounts")
  private KeyValueTable wordCountsTable;
  @UseDataSet("uniqueCount")
  private UniqueCountTable uniqueCountTable;
  @UseDataSet("wordAssocs")
  private AssociationTable associationTable;

  @Handle("getStats")
  public void getStats(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    
    // First method is getStats(); returns all the global statistics, takes no arguments
    long totalWords = 0L, uniqueWords = 0L;
    double averageLength = 0.0;

    // Read the total_length and total_words to calculate average length
    Row result = this.wordStatsTable.get(new Get("totals", "total_length", "total_words"));
    if (!result.isEmpty()) {
      
      // Extract the total sum of lengths
      long totalLength = result.getLong("total_length", 0);
      
      // Extract the total count of words
      totalWords = result.getLong("total_words", 0);
      
      // Compute the average length
      if (totalLength != 0 && totalWords != 0) {
        averageLength = (double) totalLength / (double) totalWords;
        
        // Read the unique word count
        uniqueWords = this.uniqueCountTable.readUniqueCount();
      }
    }
    // Return a map as JSON
    Map<String, Object> results = new TreeMap<String, Object>();
    results.put("totalWords", totalWords);
    results.put("uniqueWords", uniqueWords);
    results.put("averageLength", averageLength);
    responder.sendJson(new ProcedureResponse(Code.SUCCESS), results);
  }

  @Handle("getCount")
  public void getCount(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    
    // Second method is getCount() with argument of word='', optional limit=#
    // Returns count of words and top words associated with that word,
    // up to the specified limit (default limit = 10 if not specified)
    String word = request.getArgument("word");
    if (word == null) {
      responder.error(Code.CLIENT_ERROR, "Method 'getCount' requires argument 'word'");
      return;
    }

    String limitArg = request.getArgument("limit");
    int limit = limitArg == null ? 10 : Integer.valueOf(limitArg);

    // Read the word count
    byte[] countBytes = this.wordCountsTable.read(Bytes.toBytes(word));
    Long wordCount = countBytes == null ? 0L : Bytes.toLong(countBytes);

    // Read the top associated words
    Map<String, Long> wordsAssocs = this.associationTable.readWordAssocs(word, limit);

    // Return a map as JSON
    Map<String, Object> results = new TreeMap<String, Object>();
    results.put("word", word);
    results.put("count", wordCount);
    results.put("assocs", wordsAssocs);
    responder.sendJson(new ProcedureResponse(Code.SUCCESS), results);
  }

  @Handle("getAssoc")
  public void getAssoc(ProcedureRequest request, ProcedureResponder responder) throws Exception {
    
    // Third method is getAssoc() with argument of word1, word2
    // returns number of times the two words co-occurred
    String word1 = request.getArgument("word1");
    String word2 = request.getArgument("word2");
    if (word1 == null || word2 == null) {
      responder.error(Code.CLIENT_ERROR, "Method 'getCount' requires arguments 'word1' and 'word2'");
      return;
    }
    // Read the top associated words
    long count = this.associationTable.getAssoc(word1, word2);

    // Return a map as JSON
    Map<String, Object> results = new TreeMap<String, Object>();
    results.put("word1", word1);
    results.put("word2", word2);
    results.put("count", count);
    responder.sendJson(new ProcedureResponse(Code.SUCCESS), results);
  }
}
