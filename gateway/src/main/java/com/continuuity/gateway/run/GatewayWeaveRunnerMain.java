package com.continuuity.gateway.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.weave.WeaveRunnerMain;
import com.continuuity.data.security.HBaseSecureStoreUpdater;
import com.continuuity.data.security.HBaseTokenUtils;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.yarn.YarnSecureStore;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.Credentials;

import java.util.concurrent.TimeUnit;

/**
 * Run Gateway using weave.
 */
public class GatewayWeaveRunnerMain extends WeaveRunnerMain {

  public GatewayWeaveRunnerMain(CConfiguration cConf, Configuration hConf) {
    super(cConf, hConf);
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

  @Override
  protected void scheduleSecureStoreUpdate(WeaveRunner weaveRunner) {
    if (User.isHBaseSecurityEnabled(hConf)) {
      HBaseSecureStoreUpdater updater = new HBaseSecureStoreUpdater(hConf);
      weaveRunner.scheduleSecureStoreUpdate(updater, 30000L, updater.getUpdateInterval(), TimeUnit.MILLISECONDS);
    }
  }

  @Override
  protected WeavePreparer prepare(WeavePreparer preparer) {
    return preparer.withDependencies(new HBaseTableUtilFactory().get().getClass())
      .addSecureStore(YarnSecureStore.create(HBaseTokenUtils.obtainToken(hConf, new Credentials())));
  }
}
