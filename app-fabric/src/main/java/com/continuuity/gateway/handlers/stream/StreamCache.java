package com.continuuity.gateway.handlers.stream;

import com.continuuity.api.data.stream.StreamSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data2.OperationException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.mina.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Cache for Stream meta data lookups.
 */
public class StreamCache {

  private static final Logger LOG = LoggerFactory.getLogger(StreamCache.class);
  private static final Gson GSON = new Gson();

  private EndpointStrategy endpointStrategy;
  private ConcurrentHashSet<ImmutablePair<String, String>> knownStreams;
  private Store store;

  @Inject
  public StreamCache(StoreFactory factory) {
    this.knownStreams = new ConcurrentHashSet<ImmutablePair<String, String>>();
    this.store = factory.create();
  }

  /**
   * This must be called before the stream cache can be used.
   * @param endpointStrategy the end point strategy for the service discovery
   */
  public void init(EndpointStrategy endpointStrategy) {
    this.endpointStrategy = endpointStrategy;
  }

  private boolean streamExists(String accountId, String name) throws Exception {
    Preconditions.checkNotNull(endpointStrategy, "StreamCache was not initialized - endPointStrategy is null.");
      StreamSpecification spec = store.getStream(new Id.Account(accountId), name);
      String result = spec == null ? "" : new Gson().toJson(makeStreamRecord(spec.getName(), spec));
      return result != null && !result.isEmpty();
  }

  public boolean validateStream(String account, String name)
    throws OperationException {
    // check cache, if there, then we are good
    ImmutablePair<String, String> key = new ImmutablePair<String, String>(account, name);
    if (this.knownStreams.contains(key)) {
      return true;
    }

    // it is not in cache, refresh from mds
    try {
      if (!streamExists(account, name)) {
        return false;
      } else {
        this.knownStreams.add(key);
        return true;
      }
    } catch (Exception e) {
      String message = String.format("Exception when looking up stream '" +
                                       name + "' for account '" + account + "': " + e.getMessage());
      LOG.error(message);
      throw new OperationException(StatusCode.INTERNAL_ERROR, message, e);
    }
  }

  public void refreshStream(String account, String name)
    throws OperationException {
    try {
      // depending on existence, add to or remove from cache
      ImmutablePair<String, String> key = new ImmutablePair<String, String>(account, name);
      if (!streamExists(account, name)) {
        this.knownStreams.remove(key);
      } else {
        this.knownStreams.add(key);
      }
    } catch (Exception e) {
      String message = String.format("Exception when looking up stream '" +
                                       name + "' for account '" + account + "': " + e.getMessage());
      LOG.error(message);
      throw new OperationException(StatusCode.INTERNAL_ERROR, message, e);
    }
  }

  private static Map<String, String> makeStreamRecord(String name, StreamSpecification specification) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put("type", "Stream");
    builder.put("id", name);
    builder.put("name", name);
    if (specification != null) {
      builder.put("specification", GSON.toJson(specification));
    }
    return builder.build();
  }

}
