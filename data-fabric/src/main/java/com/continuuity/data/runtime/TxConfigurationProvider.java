/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data.runtime;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.TxConfiguration;
import com.google.inject.Provider;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

/**
 * Google Guice Provider for {@link org.apache.hadoop.conf.Configuration} instances.
 * Each call to {@link #get()} will
 * return a new {@link org.apache.hadoop.conf.Configuration} instance.
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
