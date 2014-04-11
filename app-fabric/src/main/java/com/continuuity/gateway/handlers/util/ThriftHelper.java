package com.continuuity.gateway.handlers.util;

import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.service.ServerException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.twill.discovery.Discoverable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to get a thrift client using a service discovery.
 */
public class ThriftHelper {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftHelper.class);

  /**
   * generic method to discover a thrift service and start up the
   * thrift transport and protocol layer.
   */
  public static TProtocol getThriftProtocol(String serviceName, EndpointStrategy endpointStrategy) throws
    ServerException {
    Discoverable endpoint = endpointStrategy.pick();
    if (endpoint == null) {
      String message = String.format("Service '%s' is not registered in discovery service.", serviceName);
      LOG.error(message);
      throw new ServerException(message);
    }
    TTransport transport = new TFramedTransport(
      new TSocket(endpoint.getSocketAddress().getHostName(), endpoint.getSocketAddress().getPort()));
    try {
      transport.open();
    } catch (TTransportException e) {
      String message = String.format("Unable to connect to thrift service %s at %s. Reason: %s",
                                     serviceName, endpoint.getSocketAddress(), e.getMessage());
      LOG.error(message);
      throw new ServerException(message, e);
    }
    // now try to connect the thrift client
    return new TBinaryProtocol(transport);
  }
}
