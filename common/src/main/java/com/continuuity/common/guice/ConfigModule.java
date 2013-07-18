/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.Networks;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Guice module to provide bindings for configurations.
 */
public final class ConfigModule extends AbstractModule {

  private final CConfiguration cConf;
  private final Configuration hConf;

  public ConfigModule() {
    this(CConfiguration.create(), new Configuration());
  }

  public ConfigModule(Configuration hConf) {
    this(CConfiguration.create(), hConf);
  }

  public ConfigModule(CConfiguration cConf) {
    this(cConf, new Configuration());
  }

  public ConfigModule(CConfiguration cConf, Configuration hConf) {
    this.cConf = cConf;
    this.hConf = hConf;
  }

  @Override
  protected void configure() {
    bind(CConfiguration.class).toInstance(cConf);
    bind(Configuration.class).toInstance(hConf);
    bind(YarnConfiguration.class).toInstance(new YarnConfiguration(hConf));
  }
}
