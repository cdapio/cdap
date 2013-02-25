package com.continuuity.examples.countcounts;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;

/**
 *
 */
public class CountCountsProcedure extends AbstractProcedure {
  private static Logger LOG = LoggerFactory.getLogger(CountCountsProcedure.class);

  @UseDataSet(CountCounts.tableName)
  CountCounterTable counters;

  @Handle("countcounts")
  public void handle(ProcedureRequest request, ProcedureResponder responder) throws OperationException, IOException {
    String method = request.getMethod();

    // Primary method is countCounts that returns a string representation of
    // the counts of each per-line word count
    if ("countCounts".equals(method)) {
      Map<Long,Long> counts = this.counters.getWordCountCounts();
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<Long,Long> count : counts.entrySet()) {
        sb.append("(" + count.getKey() + " -> " + count.getValue() + "), ");
      }
      String returnString = sb.substring(0, sb.length() - 2);

      // Write the returnString out to the response
      ProcedureResponse.Writer writer = responder.stream(
          new ProcedureResponse(ProcedureResponse.Code.SUCCESS));
      try {
        writer.write(Bytes.toBytes(returnString));
      } finally {
        writer.close();
      }
      return;
    }
    
    // Remaining methods are just simple count returning calls

    long returnCount = 0;
    
    if ("wordCount".equals(method)) {
      returnCount = this.counters.getTotalWordCount();
    } else if ("lineCount".equals(method)) {
      returnCount = this.counters.getLineCount();
    } else if ("lineLength".equals(method)) {
      returnCount = this.counters.getLineLength();
    } else {
      LOG.error("Received invalid method: " + method);
      responder.stream(new ProcedureResponse(ProcedureResponse.Code.FAILURE));
      return;
    }
    
    // Write the returnCount out to the response
    ProcedureResponse.Writer writer = responder.stream(
        new ProcedureResponse(ProcedureResponse.Code.SUCCESS));
    try {
      writer.write(Bytes.toBytes(Long.toString(returnCount)));
    } finally {
      writer.close();
    }
  }
}
