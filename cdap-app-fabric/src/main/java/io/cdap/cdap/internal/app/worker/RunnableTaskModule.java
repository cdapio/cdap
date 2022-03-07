/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker;

import com.google.inject.AbstractModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.internal.provision.ProvisionerProvider;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.security.auth.KeyManager;
import org.apache.twill.api.TwillRunnerService;

/**
 * Module for Runnable tasks.
 */
public class RunnableTaskModule extends AbstractModule {

  private final CConfiguration cConf;
  private final SConfiguration sConf;
  private final KeyManager keyManager;
  private final ProvisionerProvider provisionerProvider;
  private final TwillRunnerService twillRunnerService;

  private RunnableTaskModule(Builder builder) {
    this.cConf = builder.cConf;
    this.sConf = builder.sConf;
    this.keyManager = builder.keyManager;
    this.provisionerProvider = builder.provisionerProvider;
    this.twillRunnerService = builder.twillRunnerService;
  }

  @Override
  protected void configure() {
    bind(CConfiguration.class).toInstance(cConf);
    bind(SConfiguration.class).toInstance(sConf);
    bind(KeyManager.class).toInstance(keyManager);
    bind(ProvisionerProvider.class).toInstance(provisionerProvider);
    bind(TwillRunnerService.class).toInstance(twillRunnerService);
  }

  public static class Builder {

    private CConfiguration cConf;
    private SConfiguration sConf;
    private KeyManager keyManager;
    private ProvisionerProvider provisionerProvider;
    private TwillRunnerService twillRunnerService;

    public Builder cConf(CConfiguration cConf) {
      this.cConf = cConf;
      return this;
    }

    public Builder sConf(SConfiguration sConf) {
      this.sConf = sConf;
      return this;
    }

    public Builder keyManager(KeyManager keyManager) {
      this.keyManager = keyManager;
      return this;
    }

    public Builder provisionerProvider(ProvisionerProvider provisionerProvider) {
      this.provisionerProvider = provisionerProvider;
      return this;
    }

    public Builder twillRunnerService(TwillRunnerService twillRunnerService) {
      this.twillRunnerService = twillRunnerService;
      return this;
    }

    public RunnableTaskModule build() {
      return new RunnableTaskModule(this);
    }
  }
}
