package com.continuuity.gateway.util;

import com.continuuity.data2.OperationException;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.DataType;
import com.continuuity.app.services.ProgramId;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.mina.util.ConcurrentHashSet;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache for Stream meta data lookups.
 */
public class StreamCache {

  private static final Logger LOG = LoggerFactory.getLogger(StreamCache.class);

  private EndpointStrategy endpointStrategy;
  private ConcurrentHashSet<ImmutablePair<String, String>> knownStreams;

  @Inject
  public StreamCache() {
    this.knownStreams = new ConcurrentHashSet<ImmutablePair<String, String>>();
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
    TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
    AppFabricService.Client client = new AppFabricService.Client(protocol);
    try {
      String result = client.getDataEntity(new ProgramId(accountId, "", ""), DataType.STREAM, name);
      return result != null && !result.isEmpty();
    } finally {
      if (client.getInputProtocol().getTransport().isOpen()) {
        client.getInputProtocol().getTransport().close();
      }
      if (client.getOutputProtocol().getTransport().isOpen()) {
        client.getOutputProtocol().getTransport().close();
      }
    }
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
}
