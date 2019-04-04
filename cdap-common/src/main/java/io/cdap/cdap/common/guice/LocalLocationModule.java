/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.common.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;

/**
 * A guice module to providing binding for {@link LocationFactory} that uses local file system as the storage.
 */
public class LocalLocationModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(LocationFactory.class).to(LocalLocationFactory.class).in(Scopes.SINGLETON);
  }

  /**
   * Provider method to provide instance of {@link LocalLocationFactory}.
   */
  @Provides
  private LocalLocationFactory providesLocalLocationFactory(CConfiguration cConf) {
    return new LocalLocationFactory(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR)));
  }
}
