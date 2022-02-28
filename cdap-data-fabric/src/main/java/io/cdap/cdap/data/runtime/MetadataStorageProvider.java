/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.data.runtime;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.metadata.elastic.ElasticsearchMetadataStorage;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.dataset.DatasetMetadataStorage;

// TODO (CDAP-14918): Move binding for MetadataStorage out of DatasetsModules,
// TODO (CDAP-14918): so that data-fabric does not need to depend on metadata.
public class MetadataStorageProvider implements Provider<MetadataStorage> {

  private final Injector injector;
  private final CConfiguration cConf;

  @Inject
  MetadataStorageProvider(CConfiguration cConf, Injector injector) {
    this.cConf = cConf;
    this.injector = injector;
  }

  @Override
  public MetadataStorage get() {
    String config = cConf.get(Constants.Metadata.STORAGE_PROVIDER_IMPLEMENTATION,
        Constants.Metadata.STORAGE_PROVIDER_NOSQL);
    if (Constants.Metadata.STORAGE_PROVIDER_NOSQL.equalsIgnoreCase(config)) {
      return injector.getInstance(DatasetMetadataStorage.class);
    }
    if (Constants.Metadata.STORAGE_PROVIDER_ELASTICSEARCH.equalsIgnoreCase(config)) {
      return injector.getInstance(ElasticsearchMetadataStorage.class);
    }
    throw new IllegalArgumentException("Unsupported MetadataStorage '" + config + "'. Only '" +
        Constants.Metadata.STORAGE_PROVIDER_NOSQL + "' and '" +
        Constants.Metadata.STORAGE_PROVIDER_ELASTICSEARCH + "' are allowed.");
  }
}
