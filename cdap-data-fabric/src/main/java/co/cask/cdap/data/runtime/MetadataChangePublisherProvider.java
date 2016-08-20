/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data.runtime;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.metadata.publisher.KafkaMetadataChangePublisher;
import co.cask.cdap.data2.metadata.publisher.MetadataChangePublisher;
import co.cask.cdap.data2.metadata.publisher.NoOpMetadataChangePublisher;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;

/**
 * Metadata change publisher provider
 */
public final class MetadataChangePublisherProvider implements Provider<MetadataChangePublisher> {
  private final Injector injector;
  private final CConfiguration cConf;

  @Inject
  MetadataChangePublisherProvider(Injector injector, CConfiguration cConf) {
    this.injector = injector;
    this.cConf = cConf;
  }

  @Override
  public MetadataChangePublisher get() {
    if (cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED)) {
      return injector.getInstance(KafkaMetadataChangePublisher.class);
    }
    return injector.getInstance(NoOpMetadataChangePublisher.class);
  }
}
