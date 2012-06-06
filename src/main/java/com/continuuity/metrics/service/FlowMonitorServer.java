package com.continuuity.metrics.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.service.RegisteredService;
import com.continuuity.common.service.RegisteredServiceException;
import com.continuuity.flowmanager.StateChangeCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 */
public class FlowMonitorServer {
  private static final Logger Log = LoggerFactory.getLogger(FlowMonitorServer.class);
  private RegisteredService service;
  private final FlowMonitorHandler handler;
  private final StateChangeCallback callback;

  public FlowMonitorServer(final FlowMonitorHandler handler, final StateChangeCallback callback) {
    this.handler = handler;
    this.callback = callback;
  }

  public void start(final CConfiguration conf) {
    service = new FlowMonitorRegisteredService(handler, callback);

    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          service.start(null, conf);
        } catch (RegisteredServiceException e) {
          Log.error("Failed to start flow monitor service. Reason : {}.", e.getMessage());
        }
      }
    }).start();
  }

  public void stop() {
    try {
      service.stop(true);
    } catch (RegisteredServiceException e) {
      Log.error("Failed to stop service");
    }
  }
}
