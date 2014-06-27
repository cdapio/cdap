package com.continuuity.api;

import com.continuuity.api.data.DataSetContext;

import org.apache.twill.discovery.ServiceDiscovered;

import java.util.Map;

/**
 * This interface represents a context for a processor or elements of a processor.
 */
public interface RuntimeContext extends DataSetContext {
  /**
   * @return A map of argument key and value.
   */
  Map<String, String> getRuntimeArguments();

  /**
   * Used to discover services inside a given application and twill-service.
   * @param applicationId Application Name.
   * @param serviceId Service Name.
   * @param serviceName Announced Name.
   * @return ServiceDiscovered
   */
  ServiceDiscovered discover(String applicationId, String serviceId, String serviceName);
}
