package com.continuuity.common.discovery;

import com.netflix.curator.x.discovery.ProviderStrategy;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.server.rest.DiscoveryContext;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

/**
 * For convenience, a version of {@link DiscoveryContext} that uses a String-to-String map as the
 * payload
 */
@Provider
public class ServicePayloadContext implements
  DiscoveryContext<ServicePayload>,
  ContextResolver<DiscoveryContext<ServicePayload>> {

  private final ServiceDiscovery<ServicePayload> serviceDiscovery;
  private final ProviderStrategy<ServicePayload> providerStrategy;
  private final int instanceRefreshMs;

  public ServicePayloadContext(
    ServiceDiscovery<ServicePayload> serviceDiscovery,
    ProviderStrategy<ServicePayload> providerStrategy,
    int instanceRefreshMs) {
    this.serviceDiscovery = serviceDiscovery;
    this.providerStrategy = providerStrategy;
    this.instanceRefreshMs = instanceRefreshMs;
  }

  @Override
  public ProviderStrategy<ServicePayload> getProviderStrategy(){
    return providerStrategy;
  }

  @Override
  public int getInstanceRefreshMs(){
    return instanceRefreshMs;
  }

  @Override
  public ServiceDiscovery<ServicePayload> getServiceDiscovery(){
    return serviceDiscovery;
  }

  @Override
  public void marshallJson(ObjectNode node, String fieldName, ServicePayload
    payload) throws Exception {
//    ObjectNode objectNode = node.putObject(fieldName);
//    for(Map.Entry<String, String> entry : payload.getAll()) {
//      objectNode.put(entry.getKey(), entry.getValue());
//    }
  }

  @Override
  public ServicePayload unMarshallJson(JsonNode node) throws Exception {
    //    Iterator<Map.Entry<String, JsonNode>> fields = node.getFields();
//    while(fields.hasNext()) {
//      Map.Entry<String, JsonNode> entry = fields.next();
//      payload.add(entry.getKey(), entry.getValue().asText());
//    }
    return new ServicePayload();
  }

  @Override
  public DiscoveryContext<ServicePayload> getContext(Class<?> type) {
    return this;
  }
}