package com.continuuity.examples.twitter;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.api.procedure.ProcedureSpecification;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static com.continuuity.examples.twitter.SortedCounterTable.Counter;

public class TwitterQuery extends AbstractProcedure {

  private SortedCounterTable topHashTags;
  private CounterTable wordCounts;
  private CounterTable hashTagWordAssocs;

  public TwitterQuery() {
    super("TwitterQuery");
  }

  public ProcedureSpecification configure() {
    return ProcedureSpecification.Builder.with()
      .setName("CountQuery")
      .setDescription("Example Count Query Procedure")
      .useDataSet(TwitterFlow.topHashTags, TwitterFlow.wordCounts, TwitterFlow.hashTagWordAssocs)
      .build();
  }

  @Handle("handle")
  public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {

    final Map<String, String> args = request.getArguments();
    String method=request.getMethod();

    if (!method.equals("getTopTags")) {
      String msg = "Invalid method: " + method;
      responder.stream(new ProcedureResponse(ProcedureResponse.Code.FAILURE));
      return;
    }
    int limit = 10;
    if (args.containsKey("limit")) {
      try {
        limit = Integer.parseInt(args.get("limit"));
      } catch (NumberFormatException nfe) {}
    }
    StringBuilder sb = new StringBuilder();
    sb.append("{tags:[");
    try {
      List<Counter> topTags = this.topHashTags.readTopCounters(TwitterFlow.HASHTAG_SET, limit);
      boolean first = true;
      for (Counter topTag : topTags) {
        String tag = new String(topTag.getName());
        if (!first) sb.append(",");
        else first = false;
        sb.append("{\"tag\":\"");
        sb.append(tag);
        sb.append("\",\"count\":");
        sb.append(Long.toString(topTag.getCount()));
        sb.append(",words:[");
        Map<String,Long> assocs = this.hashTagWordAssocs.readCounterSet(tag);
        boolean sfirst = true;
        for (Map.Entry<String,Long> assoc : assocs.entrySet()) {
          if (!sfirst) sb.append(",");
          else sfirst = false;
          sb.append("{\"word\":\"");
          sb.append(assoc.getKey());
          sb.append("\",\"assoc_count\":");
          sb.append(Long.toString(assoc.getValue()));
          sb.append(",\"total_count\":");
          sb.append(Long.valueOf(
              this.wordCounts.readCounterSet(TwitterFlow.WORD_SET,
                  assoc.getKey().getBytes())));
          sb.append("}");
        }
        sb.append("]}");
      }
    } catch (OperationException e) {
      e.printStackTrace();
      String msg = "Read operation failed: " + e.getMessage();
      responder.stream(new ProcedureResponse(ProcedureResponse.Code.FAILURE));
      return;
    }
    sb.append("]}");
    ProcedureResponse.Writer writer = responder.stream(new ProcedureResponse(ProcedureResponse.Code.SUCCESS));
    try {
      writer.write(ByteBuffer.wrap(sb.toString().getBytes(Charset.forName("UTF-8"))));
    } finally {
      writer.close();
    }
    return;
  }

}
