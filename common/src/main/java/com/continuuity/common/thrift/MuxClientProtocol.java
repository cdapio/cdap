package com.continuuity.common.thrift;

import com.google.common.collect.Maps;
import org.apache.thrift.protocol.TProtocol;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <code>MuxClientProtocol</code> is helper class that registers multiple
 * services on the client side along with the protocol associated
 * with the service.
 * <blockquote>
 *   <pre>
 *     MuxClientProtocol muxClient
 *        = new MuxClientProtocol(protocol, "ServiceA", "ServiceB");
 *     . . .
 *     muxClient.add(otherprotocol, "ServiceC");
 *     muxClient.add(yetanotherprotocol, "ServiceD");
 *     . . .
 *     ServiceA.Client aClient = new ServiceA.Client(muxClient.get("ServiceA"));
 *     ServiceB.Client bClient = new ServiceB.Client(muxClient.get("ServiceB"));
 *     ServiceC.Client cClient = new ServiceC.Client(muxClient.get("ServiceC"));
 *     ServiceD.Client dClient = new ServiceD.Client(muxClient.get("ServiceD"));
 *     . . .
 *   </pre>
 * </blockquote>
 */
public class MuxClientProtocol {

  /** Map of service name to <code>MuxProtocol</code> */
  private final Map<String, MuxProtocol> protocolMap = Maps.newHashMap();

  /**
   * Constructor that allows to register a single protocol with multiple
   * service names.
   *
   * @param protocol to be associated with the service names.
   * @param serviceNames names of the services to be registered with
   *                     <code>protocol</code>
   */
  public MuxClientProtocol(TProtocol protocol, String... serviceNames) {
    for (String serviceName : serviceNames) {
      protocolMap.put(serviceName, new MuxProtocol(protocol, serviceName));
    }
  }

  /**
   * Adds a protocol and associated service.
   *
   * @param protocol used to communicated with mux service
   * @param serviceName name of the service that will use protocol.
   */
  public void add(TProtocol protocol, String serviceName) {
    protocolMap.put(serviceName, new MuxProtocol(protocol, serviceName));
  }

  /**
   * Deletes a service protocol.
   *
   * @param serviceName Name of the service to be removed.
   */
  public void delete(String serviceName) {
    if (protocolMap.containsKey(serviceName)) {
      protocolMap.remove(serviceName);
    }
  }

  /**
   * Gets a protocols associated with the <code>serviceName</code>
   *
   * @param serviceName of the service to be retrieved.
   * @return <code>MuxProtocol</code> instance associated with serviceName
   * @throws IllegalArgumentException If the service name is not found in the
   * registery of registered protocols.
   */
  public MuxProtocol get(String serviceName)
      throws IllegalArgumentException {
    if (!protocolMap.containsKey(serviceName)) {
      throw new IllegalArgumentException("Service name " + serviceName +
        " is registered");
    }
    return protocolMap.get(serviceName);
  }
}
