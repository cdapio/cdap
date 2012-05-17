package com.continuuity.overlord.metrics.client.impl;

import com.continuuity.overlord.metrics.client.FMClient;
import com.continuuity.monitoring.generated.FlowMonitor;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 *
 */
public class DefaultFMClient implements FMClient {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultFMClient.class);
  private final String hostname;
  private final int port;
  private final TTransport transport;
  private FlowMonitor.Client client = null;

  @Inject
  public DefaultFMClient(@Named("FMMetricCollector.hostname" ) String hostname,
                         @Named("FMMetricCollector.port" ) int port) throws IOException, TTransportException {
    this.hostname = hostname;
    this.port = port;
    transport =
      new TFramedTransport( new TSocket(hostname,port));
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    client = new FlowMonitor.Client(protocol);
  }

  @Override
  public boolean registerFlow(String name, String instance, String definition) {
    boolean status = true;
    try {
      if(client != null)
        client.registerFlow(name, instance, definition);
    } catch (TException e) {
      LOG.error("Failed to connect to Flow Monitor ", e);
      status = false;
    }
    return status;
  }
  
  @Override
  public boolean registerStart(String name, String instance) {
    boolean status = true;
    try {
      if(client != null)
        client.registerStart(name, instance);
    } catch (TException e) {
      LOG.error("Failed to connect to Flow Monitor ", e);
      status = false;
    }
    return status;
  }
  
  @Override
  public boolean registerStop(String name, String instance) {
    boolean status = true;
    try {
      if(client != null)  {
        client.registerStop(name, instance);
      }
    } catch (TException e) {
      LOG.error("Failed to connect to Flow Monitor", e);
      status = false;
    }
    return status;
  }

}
