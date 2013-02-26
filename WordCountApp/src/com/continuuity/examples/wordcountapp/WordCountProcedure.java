package com.continuuity.examples.wordcountapp;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;

public class WordCountProcedure extends AbstractProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(
      WordCountProcedure.class);

  @UseDataSet("wordStats")
  private Table wordStatsTable;

  @UseDataSet("wordCounts")
  private Table wordCountsTable;

  @UseDataSet("uniqueCount")
  private UniqueCountTable uniqueCountTable;

  @UseDataSet("wordAssocs")
  private WordAssocTable wordAssocTable;
 
  @Handle("wordcount")
  public void process(ProcedureRequest request, ProcedureResponder responder)
      throws IOException, OperationException {
    String method = request.getMethod();
    try {
      if (method.equals("getStats")) {

        // query 1 is 'getStats' and returns all the global statistics, no args
        // wordsSeen, uniqueWords, avgWordLength

        handleGetStats(request, responder);
        
      } else if (method.equals("getCount")) {

        // query 2 is 'getCount' with argument of word='', optional limit=#
        // returns count of word and top words associated with that word,
        // up to specified limit (default limit = 10 if not specified)
        
        handleGetCount(request, responder);
        
      } else {
        LOG.error("Invalid method: " + method);
        responder.stream(new ProcedureResponse(ProcedureResponse.Code.FAILURE));
      }
    } catch (OperationException e) {
      LOG.error("Exception during query", e);
      responder.stream(new ProcedureResponse(ProcedureResponse.Code.FAILURE));
    }
  }

  void handleGetStats(ProcedureRequest request, ProcedureResponder responder)
      throws OperationException, IOException {

    // Read the total_length and total_words to calculate average length
    OperationResult<Map<byte[],byte[]>> result = this.wordStatsTable.read(
        new Read(Bytes.toBytes("total"), Bytes.toBytes("total_length")));
    Long totalLength = result.isEmpty() ? 0L :
      Bytes.toLong(result.getValue().get(Bytes.toBytes("total_length")));
    
    result = this.wordStatsTable.read(
        new Read(Bytes.toBytes("total"), Bytes.toBytes("total_words")));
    Long totalWords = result.isEmpty() ? 0L :
      Bytes.toLong(result.getValue().get(Bytes.toBytes("total_words")));
    Double avgLength = new Double(totalLength) / new Double(totalWords);

    // Read the unique word count
    Long uniqueWords = this.uniqueCountTable.readUniqueCount();

    // Construct and return the JSON string
    String ret = "{'wordsSeen':" + totalWords + ",'uniqueWords':" +
          uniqueWords + ",'avgLength':" + avgLength + "}";
    ProcedureResponse.Writer writer = responder.stream(
        new ProcedureResponse(ProcedureResponse.Code.SUCCESS));
    try {
      writer.write(Bytes.toBytes(ret));
    } finally {
      writer.close();
    }
  }

  void handleGetCount(ProcedureRequest request, ProcedureResponder responder)
      throws OperationException, IOException {

    final Map<String, String> arguments = request.getArguments();
    if (!arguments.containsKey("word")) {
      String msg = "Method 'getCount' requires argument 'word'";
      LOG.error(msg);
      responder.stream(
          new ProcedureResponse(ProcedureResponse.Code.FAILURE));
    }

    // Parse the arguments
    String word = arguments.get("word");
    Integer limit = arguments.containsKey("limit") ?
        Integer.valueOf(arguments.get("limit")) : 10;

    // Read the word count
    OperationResult<Map<byte[],byte[]>> result = this.wordCountsTable.read(
        new Read(Bytes.toBytes(word), Bytes.toBytes("total")));
    Long wordCount = result.isEmpty() ? 0L :
      Bytes.toLong(result.getValue().get(Bytes.toBytes("total")));

    // Read the top associated words
    Map<String,Long> wordsAssocs =
        this.wordAssocTable.readWordAssocs(word, limit);

    // Construct and return the JSON string
    StringBuilder builder = new StringBuilder("{'word':'" + word +
        "','count':" + wordCount + "," + "'assocs':[");
    boolean first = true;
    for (Map.Entry<String, Long> wordAssoc : wordsAssocs.entrySet()) {
      if (!first) builder.append(",");
      else first = false;
      builder.append("{'word':'" + wordAssoc.getKey() + "',");
      builder.append("'count':" + wordAssoc.getValue() + "}");
    }
    builder.append("]}");
    ProcedureResponse.Writer writer = responder.stream(
        new ProcedureResponse(ProcedureResponse.Code.SUCCESS));
    try {
      writer.write(Bytes.toBytes(builder.toString()));
    } finally {
      writer.close();
    }

  }
}
