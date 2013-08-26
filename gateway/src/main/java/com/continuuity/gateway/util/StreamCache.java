package com.continuuity.gateway.util;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Stream;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Cache for Stream meta data lookups.
 */
public class StreamCache {

  private static final Logger LOG = LoggerFactory
    .getLogger(StreamCache.class);

  private MetadataService mds;
  private ConcurrentMap<ImmutablePair<String, String>, Stream> knownStreams;

  @Inject
  public StreamCache(MetadataService mds) {
    this.mds = mds;
    this.knownStreams = new
      ConcurrentHashMap<ImmutablePair<String, String>, Stream>();
  }

  public boolean validateStream(String account, String name)
    throws OperationException {
    // check cache, if there, then we are good
    ImmutablePair<String, String> key =
      new ImmutablePair<String, String>(account, name);
    if (this.knownStreams.containsKey(key)) {
      return true;
    }

    // it is not in cache, refresh from mds
    Stream stream;
    try {
      stream = this.mds.getStream(new Account(account), new Stream(name));
    } catch (Exception e) {
      String message = String.format("Exception when looking up stream '" +
                                       name + "' for account '" + account + "': " + e.getMessage());
      LOG.error(message);
      throw new OperationException(StatusCode.INTERNAL_ERROR, message, e);
    }
    if (stream == null || !stream.isExists()) {
      return false;
    } else {
      this.knownStreams.putIfAbsent(key, stream);
      return true;
    }
  }

  public void refreshStream(String account, String name)
    throws OperationException {
    // read entry from mds
    Stream stream;
    try {
      stream = this.mds.getStream(new Account(account), new Stream(name));
    } catch (Exception e) {
      String message = String.format("Exception when looking up stream '" +
                                       name + "' for account '" + account + "': " + e.getMessage());
      LOG.error(message);
      throw new OperationException(StatusCode.INTERNAL_ERROR, message, e);
    }
    // depending on existence, add to or remove from cache
    ImmutablePair<String, String> key =
      new ImmutablePair<String, String>(account, name);
    if (stream == null || !stream.isExists()) {
      this.knownStreams.remove(key);
    } else {
      this.knownStreams.put(key, stream);
    }
  }
}
