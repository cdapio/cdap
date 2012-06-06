package com.continuuity.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.options.Option;
import com.continuuity.common.options.OptionsParser;
import com.continuuity.metrics.service.FlowMonitorClient;
import com.continuuity.metrics.stubs.FlowMetric;

import java.util.Random;

/**
 * A Test client for generating data - nothing fancy here.
 */
public class FlowMonitorDataGenerator {
  @Option
  private String zookeeper = null;

  @Option (name = "genmetrics")
  private boolean genMetrics = true;

  public void doMain(String args[]) {
    OptionsParser.init(this, args, System.out);
    try {
      CConfiguration conf = CConfiguration.create();
      if(zookeeper != null) {
        conf.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, zookeeper);
      } else {
        conf.set(Constants.CFG_ZOOKEEPER_ENSEMBLE, Constants.DEFAULT_ZOOKEEPER_ENSEMBLE);
      }
      FlowMonitorClient client = new FlowMonitorClient(conf);
      if(genMetrics) {
        generateMetrics(client);
        System.exit(0);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void generateMetrics(FlowMonitorClient client) {
    String[] accountIds = { "demo"};
    String[] rids = { "ABC", "XYZ"};
    String[] applications = { "personalization"};
    String[] versions = { "18", "28"};
    String[] flows = { "targetting", "smart-explorer", "indexer"};
    String[] flowlets = { "flowlet-A", "flowlet-B", "flowlet-C", "flowlet-D"};
    String[] metrics = { "in", "out", "ops"};
    String[] instances = { "1", "2"};

    Random random = new Random();
    for(int i = 0; i < 100; ++i) {
      String rid = rids[(i % rids.length)];
      for(int i1 = 0; i1 < accountIds.length; ++i1) {
        int timestamp = (int)(System.currentTimeMillis()/1000);
        for(int i2 = 0; i2 < applications.length; ++i2) {
          for(int i3 = 0; i3 < versions.length; ++i3) {
            for(int i4 = 0; i4 < flows.length; ++i4) {
              for(int i5 = 0; i5 < flowlets.length; ++i5) {
                for(int i6 = 0; i6 < instances.length; ++i6) {
                  for(int i7 = 0; i7 < metrics.length; ++i7) {
                    FlowMetric metric = new FlowMetric();
                    metric.setInstance(instances[i6]);
                    metric.setFlowlet(flowlets[i5]);
                    metric.setFlow(flows[i4]);
                    metric.setVersion(versions[i3]);
                    metric.setApplication(applications[i2]);
                    metric.setAccountId(accountIds[i1]);
                    metric.setRid(rid);
                    metric.setMetric(metrics[i7]);
                    metric.setTimestamp(timestamp);
                    metric.setValue(1000 * random.nextInt() );
                    client.add(metric);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  public static void main(String[] args) {
    FlowMonitorDataGenerator dg = new FlowMonitorDataGenerator();
    dg.doMain(args);
  }
}
