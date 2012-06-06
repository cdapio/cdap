package com.continuuity.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.options.Option;
import com.continuuity.common.options.OptionsParser;
import com.continuuity.common.service.RegisteredService;
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
    OptionsParser.init(this, args, System.out);
    try {
      CConfiguration conf = CConfiguration.create();
      if(zookeeper != null) {
        conf.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zookeeper);
      } else {
        conf.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, Constants.DEFAULT_ZOOKEEPER_ENSEMBLE);
      }

      Injector injector = Guice.createInjector(DIModules.getFileHSQLBindings());
      RegisteredService service = injector.getInstance(RegisteredService.class);
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
