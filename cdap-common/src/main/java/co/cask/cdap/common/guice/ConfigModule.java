/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.common.guice;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.CConfigurationUtil;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.http.DefaultHttpRequestConfig;
import co.cask.cdap.common.utils.Networks;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Guice module to provide bindings for configurations.
 */
public final class ConfigModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigModule.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final SConfiguration sConf;

  public ConfigModule() {
    this(CConfiguration.create(), new Configuration(), SConfiguration.create());
  }

  public ConfigModule(Configuration hConf) {
    this(CConfiguration.create(), hConf, SConfiguration.create());
  }

  public ConfigModule(CConfiguration cConf) {
    this(cConf, new Configuration(), SConfiguration.create());
  }


  public ConfigModule(CConfiguration cConf, Configuration hConf) {
    this(cConf, hConf, SConfiguration.create());
  }

  public ConfigModule(CConfiguration cConf, Configuration hConf, SConfiguration sConf) {
    CConfigurationUtil.verify(cConf);
    this.cConf = cConf;
    this.hConf = hConf;
    this.sConf = sConf;
    CConfigurationUtil.copyTxProperties(cConf, hConf);

    // Set system properties for all HTTP requests if they were found
    String connectionTimeout = cConf.get(Constants.HTTP_CLIENT_CONNECTION_TIMEOUT_MS);
    String readTimeout = cConf.get(Constants.HTTP_CLIENT_READ_TIMEOUT_MS);

    if (connectionTimeout != null) {
      System.setProperty(DefaultHttpRequestConfig.CONNECTION_TIMEOUT_PROPERTY_NAME, connectionTimeout);
    } else {
      LOG.warn("Configuration for {} not found. Falling back to default value of {}",
               Constants.HTTP_CLIENT_CONNECTION_TIMEOUT_MS, DefaultHttpRequestConfig.DEFAULT_TIMEOUT);
    }

    if (readTimeout != null) {
      System.setProperty(DefaultHttpRequestConfig.READ_TIMEOUT_PROPERTY_NAME, readTimeout);
    } else {
      LOG.warn("Configuration for {} not found. Falling back to default value of {}",
               Constants.HTTP_CLIENT_READ_TIMEOUT_MS, DefaultHttpRequestConfig.DEFAULT_TIMEOUT);
    }
  }

  @Override
  protected void configure() {
    bind(CConfiguration.class).toInstance(cConf);
    bind(Configuration.class).toInstance(hConf);
    bind(SConfiguration.class).toInstance(sConf);
    bind(YarnConfiguration.class).toInstance(new YarnConfiguration(hConf));
  }

  @Provides
  @Named(Constants.AppFabric.SERVER_ADDRESS)
  @SuppressWarnings("unused")
  public InetAddress providesHostname(CConfiguration cConf) {
    return Networks.resolve(cConf.get(Constants.AppFabric.SERVER_ADDRESS),
                            new InetSocketAddress("localhost", 0).getAddress());
  }
}
