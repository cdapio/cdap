package com.continuuity.examples.twitter;

import java.util.List;
import java.util.Map;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.query.QueryProvider;
import com.continuuity.api.query.QueryProviderContentType;
import com.continuuity.api.query.QueryProviderResponse;
import com.continuuity.api.query.QueryProviderResponse.Status;
import com.continuuity.api.query.QuerySpecifier;

import static com.continuuity.examples.twitter.SortedCounterTable.Counter;

public class TwitterQuery extends QueryProvider {

  @Override
  public void configure(QuerySpecifier specifier) {
    specifier.service("twitter");
    specifier.timeout(20000);
    specifier.dataset(TwitterFlow.topHashTags);
    specifier.dataset(TwitterFlow.wordCounts);
    specifier.dataset(TwitterFlow.hashTagWordAssocs);
    specifier.type(QueryProviderContentType.JSON);
    specifier.provider(TwitterQuery.class);
  }

  private SortedCounterTable topHashTags;

  private CounterTable wordCounts;
  private CounterTable hashTagWordAssocs;

  @Override
  public void initialize() {
    this.topHashTags = getQueryProviderContext().getDataSet(TwitterFlow.topHashTags);
    this.wordCounts = getQueryProviderContext().getDataSet(TwitterFlow.wordCounts);
    this.hashTagWordAssocs = getQueryProviderContext().getDataSet(TwitterFlow.hashTagWordAssocs);
  }

  @Override
  public QueryProviderResponse process(String method,
      Map<String, String> args) {
    if (!method.equals("getTopTags")) {
      String msg = "Invalid method: " + method;
      return new QueryProviderResponse(Status.FAILED, msg, msg);
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
      return new QueryProviderResponse(Status.FAILED, msg, msg);
    }
    sb.append("]}");
    return new QueryProviderResponse(sb.toString());
  }

}
