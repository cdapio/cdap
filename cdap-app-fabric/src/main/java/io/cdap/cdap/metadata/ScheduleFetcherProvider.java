/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;

/**
 * Provider for {@link PreferencesFetcher}.
 * Use {@link LocalPreferencesFetcherInternal} if storage implication is {@link Constants.Dataset#DATA_STORAGE_SQL}
 * Use {@link RemotePreferencesFetcherInternal} if storage implication is {@link Constants.Dataset#DATA_STORAGE_NOSQL}
 */
public class ScheduleFetcherProvider implements Provider<ScheduleFetcher> {
  private final CConfiguration cConf;
  private final Injector injector;

  @Inject
  ScheduleFetcherProvider(CConfiguration cConf, Injector injector) {
    this.cConf = cConf;
    this.injector = injector;
  }

  @Override
  public ScheduleFetcher get() {
    String storageImpl = cConf.get(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION);
    if (storageImpl == null) {
      throw new IllegalStateException("No storage implementation is specified in the configuration file");
    }

    storageImpl = storageImpl.toLowerCase();
    if (storageImpl.equals(Constants.Dataset.DATA_STORAGE_NOSQL)) {
      return injector.getInstance(RemoteScheduleFetcher.class);
    }
    if (storageImpl.equals(Constants.Dataset.DATA_STORAGE_SQL)) {
      return injector.getInstance(LocalScheduleFetcher.class);
    }
    throw new UnsupportedOperationException(
      String.format("%s is not a supported storage implementation, the supported implementations are %s and %s",
                    storageImpl, Constants.Dataset.DATA_STORAGE_NOSQL, Constants.Dataset.DATA_STORAGE_SQL));
  }
}

