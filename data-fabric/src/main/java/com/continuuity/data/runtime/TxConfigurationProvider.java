package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.TxConfiguration;
import com.google.inject.Provider;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

/**
 *
 */
public class TxConfigurationProvider implements Provider<Configuration> {
  private final CConfiguration cConf;
  private final Configuration hConf;

  @Inject
  public TxConfigurationProvider(CConfiguration config, Configuration hConf) {
    this.cConf = config;
    this.hConf = hConf;
  }

  @Override
  public Configuration get() {
    Configuration configuration = TxConfiguration.create();
    cConf.copyTo(configuration);
    return configuration;
  }
}
