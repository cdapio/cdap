package com.continuuity.metrics.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.ServiceDiscoveryClient;
import com.continuuity.common.discovery.ServiceDiscoveryClientException;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.metrics.stubs.*;
import com.netflix.curator.x.discovery.ProviderStrategy;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.strategies.RandomStrategy;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 *
 *
 */
public class MetricsClient implements Closeable {
  private static final Logger Log = LoggerFactory.getLogger(MetricsClient.class);
  private ServiceDiscoveryClient serviceDiscoveryClient;
  private FlowMonitor.Client client;
  private static final int MAX_RETRY = 3;
  private ImmutablePair<String, Integer> endpoint;

  public MetricsClient(String host, int port) {
    endpoint = new ImmutablePair<String, Integer>(host, port);
    client = connect(false);
  }

  public MetricsClient(CConfiguration configuration) throws ServiceDiscoveryClientException {
    String zkEnsemble = configuration.get(Constants.CFG_ZOOKEEPER_ENSEMBLE, Constants.DEFAULT_ZOOKEEPER_ENSEMBLE);
    serviceDiscoveryClient = new ServiceDiscoveryClient(zkEnsemble);
    client = connect(true);
    if (client == null) {
      throw new ServiceDiscoveryClientException("No services available");
    }
  }

  private ImmutablePair<String, Integer> getServiceEndpoint() {
    ProviderStrategy<ServiceDiscoveryClient.ServicePayload> strategy =
      new RandomStrategy<ServiceDiscoveryClient.ServicePayload>();
    ServiceDiscoveryClient.ServiceProvider provider = null;
    try {
      provider = serviceDiscoveryClient.getServiceProvider("flow-monitor");
      if (provider.getInstances().size() < 1) {
        return null;
      }
    } catch (ServiceDiscoveryClientException e) {
      Log.error("Unable to retrieve service information. Reason : {}", e.getMessage());
      return null;
    } catch (Exception e) {
      Log.error("Issue retrieving service list for service flow-monitor from service discovery.");
      return null;
    }

    ServiceInstance<ServiceDiscoveryClient.ServicePayload> instance = null;
    try {
      instance = strategy.getInstance(provider);
      if (instance != null) {
        return new ImmutablePair<String, Integer>(instance.getAddress(), instance.getPort());
      }
    } catch (Exception e) {
      Log.error("Unable to retrieve an instance to connect to for service 'flow-monitor'. Reason : {}",
        e.getMessage());
    }
    return null;
  }

  private FlowMonitor.Client connect(boolean autoDiscovery) {

    if(autoDiscovery) {
      endpoint = getServiceEndpoint();
    }

    if (endpoint == null) {
      return null;
    }

    TTransport transport = new TFramedTransport(
      new TSocket(endpoint.getFirst(), endpoint.getSecond()));
    try {
      transport.open();
    } catch (TTransportException e) {
      return null;
    }
    TProtocol protocol = new TBinaryProtocol(transport);
    return new FlowMonitor.Client(protocol);
  }

  public void add(FlowMetric metric) {
    int i = 0;

    while (i < MAX_RETRY) {
      boolean exception = false;
      try {
        client.add(metric);
        return;
      } catch (TException e) {
        exception = true;
      }
      if (exception) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
      }
    }
    Log.warn("Unable to send metrics ");
  }


  public List<FlowState> getFlows(String accountId) throws TException {
    return client.getFlows(accountId);
  }

  public List<FlowRun> getFlowHistory(String accountId, String appId, String flowId) throws TException {
    return client.getFlowHistory(accountId, appId, flowId);
  }

  public String getFlowDefinition(String accountId, String appId, String flowId, String versionId) throws TException {
    return client.getFlowDefinition(accountId, appId, flowId, versionId);
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   *
   * @throws java.io.IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    if(serviceDiscoveryClient != null) {
      serviceDiscoveryClient.close();
    }
  }
}
