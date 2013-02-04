package CountCounts;

import com.continuuity.api.query.QueryProvider;
import com.continuuity.api.query.QueryProviderContentType;
import com.continuuity.api.query.QueryProviderResponse;
import com.continuuity.api.query.QuerySpecifier;

import java.util.Map;

/**
 *
 */
public class CountQuery extends QueryProvider {

  CounterTable counters;

  @Override
  public void configure(QuerySpecifier specifier) {
    specifier.service("GetCount");
    specifier.dataset(Common.tableName);
    specifier.type(QueryProviderContentType.TEXT);
    specifier.provider(this.getClass());
  }

  @Override
  public void initialize() {
    super.initialize();
    this.counters = getQueryProviderContext().getDataSet(Common.tableName);
  }

  @Override
  public QueryProviderResponse process(String method, Map<String, String> arguments) {
    try {
      long count = 0;
      if ("total".equals(method)) {
        String what = arguments.get("key");
        if ("sink".equals(what)) {
          count = this.counters.get(Incrementer.keyTotal);
        }
        else if ("source".equals(what)) {
          count = this.counters.get(StreamSource.keyTotal);
        }
        else {
          return new QueryProviderResponse(QueryProviderResponse.Status.FAILED, "Bad Arguments",
                                           "Parameter 'key' must be either 'sink' or 'source'");
        }
      }
      else if ("count".equals(method)) {
        String key = arguments.get("words");
        if (key != null) {
          try {
            Integer.parseInt(key);
          } catch (NumberFormatException e) {
            key = null;
          }
        }
        if (key == null) {
          return new QueryProviderResponse(QueryProviderResponse.Status.FAILED, "Bad Arguments",
                                           "Parameter 'words' must be a number.");
        }
        count = this.counters.get(key);
      }
      return new QueryProviderResponse(Long.toString(count));
    }
    catch (Exception e) {
      return new QueryProviderResponse(QueryProviderResponse.Status.FAILED,
                                       "Caught Exception", e.getMessage());
    }
  }
}
