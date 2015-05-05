/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy.pipeline.adapter;

import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.data.dataset.DatasetCreationSpec;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.AdapterDefinition;
import com.google.common.reflect.TypeToken;

/**
 * Adds a configured adapter to the store.
 */
public class AdapterRegistrationStage extends AbstractStage<AdapterDefinition> {

  private final Store store;
  private final Id.Namespace namespace;
  private final UsageRegistry usageRegistry;

  public AdapterRegistrationStage(Id.Namespace namespace, Store store, UsageRegistry usageRegistry) {
    super(TypeToken.of(AdapterDefinition.class));
    this.store = store;
    this.namespace = namespace;
    this.usageRegistry = usageRegistry;
  }

  @Override
  public void process(AdapterDefinition input) throws Exception {
    store.addAdapter(namespace, input);

    Id.Adapter adapterId = Id.Adapter.from(namespace, input.getName());
    for (StreamSpecification stream : input.getStreams().values()) {
      usageRegistry.register(adapterId, Id.Stream.from(namespace, stream.getName()));
    }
    for (DatasetCreationSpec dataset : input.getDatasets().values()) {
      usageRegistry.register(adapterId, Id.DatasetInstance.from(namespace, dataset.getInstanceName()));
    }
    emit(input);
  }
}
