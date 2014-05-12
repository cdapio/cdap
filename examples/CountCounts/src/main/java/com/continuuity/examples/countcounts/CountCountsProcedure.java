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
package com.continuuity.examples.countcounts;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
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
