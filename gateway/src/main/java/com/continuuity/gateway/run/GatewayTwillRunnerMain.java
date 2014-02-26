package com.continuuity.gateway.run;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.twill.TwillRunnerMain;
import com.continuuity.data.security.HBaseSecureStoreUpdater;
import com.continuuity.data.security.HBaseTokenUtils;
import com.continuuity.data2.util.hbase.HBaseTableUtilFactory;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.Credentials;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.yarn.YarnSecureStore;

import java.util.concurrent.TimeUnit;

/**
 * Run Gateway using twill.
 */
public class GatewayTwillRunnerMain extends TwillRunnerMain {

  public GatewayTwillRunnerMain(CConfiguration cConf, Configuration hConf) {
    super(cConf, hConf);
  }

  public static void main(String[] args) throws Exception {
    new GatewayTwillRunnerMain(CConfiguration.create(), HBaseConfiguration.create()).doMain(args);
  }

  @Override
  protected TwillApplication createTwillApplication() {
    try {
      return new GatewayTwillApplication(cConf,
                                         getSavedCConf(),
                                         getSavedHConf());

    } catch (Exception e) {
      throw  Throwables.propagate(e);
    }
  }

  @Override
  protected void scheduleSecureStoreUpdate(TwillRunner twillRunner) {
    if (User.isHBaseSecurityEnabled(hConf)) {
      HBaseSecureStoreUpdater updater = new HBaseSecureStoreUpdater(hConf);
      twillRunner.scheduleSecureStoreUpdate(updater, 30000L, updater.getUpdateInterval(), TimeUnit.MILLISECONDS);
    }
  }

  @Override
  protected TwillPreparer prepare(TwillPreparer preparer) {
    return preparer.withDependencies(new HBaseTableUtilFactory().get().getClass())
      .addSecureStore(YarnSecureStore.create(HBaseTokenUtils.obtainToken(hConf, new Credentials())));
  }
}
