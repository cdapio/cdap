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

package com.continuuity.examples.countcounts;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.api.metrics.Metrics;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Count Counts Procedure. Returns word count, line count and line Length.
 */
public class CountCountsProcedure extends AbstractProcedure {
  private static final Logger LOG = LoggerFactory.getLogger
                                                  (CountCountsProcedure.class);

  @UseDataSet(CountCounts.TABLE_NAME)
  CountCounterTable counters;

  private Metrics metric;

  @Handle("countCounts")
  public void countCounts(ProcedureRequest request,
                          ProcedureResponder responder) throws IOException {

    // Primary method is countCounts that returns a string representation of
    // the counts of each per-line word count
    Map<Long, Long> counts = this.counters.getWordCountCounts();

    ObjectMapper mapper = new ObjectMapper();
    byte[] returnBytes = mapper.writeValueAsBytes(counts);

    // Write the returnBytes out to the response
    ProcedureResponse.Writer writer = responder.stream(new ProcedureResponse(ProcedureResponse.Code.SUCCESS));
    try {
      metric.count("bytes.returned", returnBytes.length);
      writer.write(returnBytes);
    } finally {
      writer.close();
    }
  }

  @Handle("wordCount")
  public void wordCount(ProcedureRequest request,
                        ProcedureResponder responder) throws IOException {
    writeCountInResponse(responder, this.counters.getTotalWordCount());
  }

  @Handle("lineCount")
  public void lineCount(ProcedureRequest request,
                        ProcedureResponder responder) throws IOException {
    writeCountInResponse(responder, this.counters.getLineCount());
  }

  @Handle("lineLength")
  public void lineLength(ProcedureRequest request,
                         ProcedureResponder responder) throws IOException {
    writeCountInResponse(responder, this.counters.getLineLength());
  }

  private void writeCountInResponse(ProcedureResponder responder, long returnCount) throws IOException {
    // Write the returnCount out to the response
    ProcedureResponse.Writer writer = responder.stream(new ProcedureResponse(ProcedureResponse.Code.SUCCESS));
    try {
      writer.write(Bytes.toBytes(Long.toString(returnCount)));
    } finally {
      writer.close();
    }
  }
}
