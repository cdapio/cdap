package com.continuuity.common.discovery;

import com.google.gson.Gson;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.details.InstanceSerializer;

/**
 * ServicePayload serializer.
 */
public class ServicePayloadSerializer implements
  InstanceSerializer<ServicePayload> {
  /**
   * Instance of gson that is used to serialize the {@link ServicePayload}
   */
  private final Gson gson;
  {
    gson = new Gson();
  }

  /**
   * Serialize an instance into bytes
   *
   * @param instance the instance
   * @return byte array representing the instance
   * @throws Exception any errors
   */
  @Override
  public byte[] serialize(ServiceInstance<ServicePayload> instance) throws Exception {
    return gson.toJson(instance).getBytes();
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
    return gson.fromJson(new String(bytes), ServiceInstance.class);
  }
}
