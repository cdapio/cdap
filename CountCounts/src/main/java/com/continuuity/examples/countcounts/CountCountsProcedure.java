package com.continuuity.examples.countcounts;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.ObjectMapper;
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

  @Handle("countCounts")
  public void countCounts(ProcedureRequest request, ProcedureResponder responder)
    throws OperationException, IOException {

    // Primary method is countCounts that returns a string representation of
    // the counts of each per-line word count
    Map<Long,Long> counts = this.counters.getWordCountCounts();

    ObjectMapper mapper = new ObjectMapper();
    byte[] returnBytes = mapper.writeValueAsBytes(counts);

    // Write the returnBytes out to the response
    ProcedureResponse.Writer writer = responder.stream(
        new ProcedureResponse(ProcedureResponse.Code.SUCCESS));
    try {
      writer.write(returnBytes);
    } finally {
      writer.close();
    }
  }

  @Handle("wordCount")
  public void wordCount(ProcedureRequest request, ProcedureResponder responder)
    throws OperationException, IOException {
      writeCountInResponse(responder, this.counters.getTotalWordCount());
  }

  @Handle("lineCount")
  public void lineCount(ProcedureRequest request, ProcedureResponder responder)
    throws OperationException, IOException {
      writeCountInResponse(responder, this.counters.getLineCount());
  }

  @Handle("lineLength")
  public void lineLength(ProcedureRequest request, ProcedureResponder responder)
    throws OperationException, IOException {
      writeCountInResponse(responder, this.counters.getLineLength());
  }

  private void writeCountInResponse(ProcedureResponder responder, long returnCount) throws IOException {
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
