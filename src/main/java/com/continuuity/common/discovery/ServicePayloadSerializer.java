package com.continuuity.common.discovery;

import com.continuuity.common.serializer.JSONSerializer;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.details.InstanceSerializer;

/**
 * ServicePayload serializer.
 */
public class ServicePayloadSerializer implements
  InstanceSerializer<ServicePayload> {

  private final JSONSerializer<ServiceInstance<ServicePayload>> serializer =
    new JSONSerializer<ServiceInstance<ServicePayload>>();

  /**
   * Serialize an instance into bytes
   *
   * @param instance the instance
   * @return byte array representing the instance
   * @throws Exception any errors
   */
  @Override
  public byte[] serialize(ServiceInstance<ServicePayload> instance) throws Exception {
    return serializer.serialize(instance);
  }

  /**
   * Deserialize a byte array into an instance
   *
   * @param bytes the bytes
   * @return service instance
   * @throws Exception any errors
   */
  @Override
  @SuppressWarnings("unchecked")
  public ServiceInstance<ServicePayload> deserialize(byte[] bytes) throws Exception {
    return serializer.deserialize(bytes, ServiceInstance.class);
  }
}
