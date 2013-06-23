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

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.examples.twitter.SortedCounterTable.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TwitterProcedure extends AbstractProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(TwitterProcedure
                                                            .class);
  @UseDataSet(TwitterScanner.TOP_HASH_TAGS)
  private SortedCounterTable topHashTags;
  @UseDataSet(TwitterScanner.WORD_COUNTS)
  private CounterTable wordCounts;
  @UseDataSet(TwitterScanner.HASH_TAG_WORD_ASSOCS)
  private CounterTable hashTagWordAssocs;

  @Handle("twitter")
  public void handle(ProcedureRequest request, ProcedureResponder responder)
    throws IOException {

    final Map<String, String> args = request.getArguments();
    String method = request.getMethod();

    if (!method.equals("getTopTags")) {
      String msg = "Invalid method: " + method;
      LOG.error(msg);
      responder.stream(new ProcedureResponse(ProcedureResponse.Code.FAILURE));
      return;
    }
    int limit = 10;
    if (args.containsKey("limit")) {
      try {
        limit = Integer.parseInt(args.get("limit"));
      } catch (NumberFormatException nfe) {
      }
    }
    StringBuilder sb = new StringBuilder();
    sb.append("{tags:[");
    try {
      List<Counter> topTags = this.topHashTags.readTopCounters(
        TwitterScanner.HASHTAG_SET, limit);
      boolean first = true;
      for (Counter topTag : topTags) {
        String tag = new String(topTag.getName());

        if (!first) {
          sb.append(",");
        } else {
          first = false;
        }

        sb.append("{\"tag\":\"");
        sb.append(tag);
        sb.append("\",\"count\":");
        sb.append(Long.toString(topTag.getCount()));
        sb.append(",words:[");
        Map<String, Long> assocs = this.hashTagWordAssocs.readCounterSet(tag);
        boolean sfirst = true;
        for (Map.Entry<String, Long> assoc : assocs.entrySet()) {

          if (!sfirst) {
            sb.append(",");
          } else {
            sfirst = false;
          }

          sb.append("{\"word\":\"");
          sb.append(assoc.getKey());
          sb.append("\",\"assoc_count\":");
          sb.append(Long.toString(assoc.getValue()));
          sb.append(",\"total_count\":");
          sb.append(Long.valueOf(
            this.wordCounts.readCounterSet(TwitterScanner.WORD_SET,
                                           assoc.getKey().getBytes())));
          sb.append("}");
        }
        sb.append("]}");
      }
    } catch (OperationException e) {
      String msg = "Read operation failed: " + e.getMessage();
      LOG.error(msg, e);
      responder.stream(new ProcedureResponse(ProcedureResponse.Code.FAILURE));
      return;
    }
    sb.append("]}");
    ProcedureResponse.Writer writer = responder.stream(new ProcedureResponse(ProcedureResponse.Code.SUCCESS));
    try {
      writer.write(Bytes.toBytes(sb.toString()));
    } finally {
      writer.close();
    }
    return;
  }

}
