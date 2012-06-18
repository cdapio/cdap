package com.continuuity.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.options.Option;
import com.continuuity.common.options.OptionsParser;
import com.continuuity.metrics.service.MetricsServer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flow Monitor command line
 */
public class FlowMonitorMain {
  private static final Logger Log = LoggerFactory.getLogger(FlowMonitorMain.class);

  @Option
  private String zookeeper = null;

  public void doMain(String args[]) {
    OptionsParser.init(this, args, "Flow Monitor", "0.1.0", System.out);
    try {
      CConfiguration conf = CConfiguration.create();
      if (zookeeper != null) {
        conf.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zookeeper);
      } else {
        conf.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, Constants.DEFAULT_ZOOKEEPER_ENSEMBLE);
      }

      Injector injector = Guice.createInjector(new MetricsModules().getSingleNodeModules());
      MetricsServer service = injector.getInstance(MetricsServer.class);
      service.start(args, conf);
    } catch (Exception e) {
      Log.error("Server failed to start. Reason : {}", e.getMessage());
    }
  }

  public static void main(String[] args) {
    FlowMonitorMain serviceMain = new FlowMonitorMain();
    serviceMain.doMain(args);
  }
}
