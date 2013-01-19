package com.continuuity.common.discovery;

import com.google.gson.Gson;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.details.InstanceSerializer;

import java.nio.charset.Charset;

/**
 * ServicePayload serializer.
 */
public class ServicePayloadSerializer implements
  InstanceSerializer<ServicePayload> {

  /**
   * Charset used for json encode/decode.
   */
  private static final Charset UTF8 = Charset.forName("UTF-8");

  /**
   * Instance of gson that is used to serialize the {@link ServicePayload}
   */
  private final Gson gson = new Gson();

  /**
   * Serialize an instance into bytes
   *
   * @param instance the instance
   * @return byte array representing the instance
   * @throws Exception any errors
   */
  @Override
  public byte[] serialize(ServiceInstance<ServicePayload> instance) throws Exception {
    return gson.toJson(instance).getBytes(UTF8);
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
    return gson.fromJson(new String(bytes, UTF8), ServiceInstance.class);
  }
}
