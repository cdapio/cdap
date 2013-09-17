package com.continuuity.gateway.v2.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.weave.api.WeaveApplication;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Run Gateway using weave.
 */
public class GatewayWeaveRunnerMain extends WeaveRunnerMain {
  private final CConfiguration cConf;

  public GatewayWeaveRunnerMain(CConfiguration cConf, Configuration hConf) {
    super(cConf, hConf);
    this.cConf = cConf;
  }

  public static void main(String[] args) throws Exception {
    new GatewayWeaveRunnerMain(CConfiguration.create(), HBaseConfiguration.create()).doMain(args);
  }

  @Override
  protected WeaveApplication createWeaveApplication() {
    try {
      return new GatewayWeaveApplication(cConf,
                                         getSavedCConf(),
                                         getSavedHConf());

    } catch (Exception e) {
      throw  Throwables.propagate(e);
    }
  }

}
