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

package co.cask.cdap.internal.app.runtime.spark.serialization;

import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.proto.Id;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 *
 */
public class InMemorySerializableServiceDiscoverer extends SerializableServiceDiscoverer {
  private static final long serialVersionUID = 6547316362453719580L;
  private static final Injector injector = Guice.createInjector(new DiscoveryRuntimeModule().getInMemoryModules());

  private final transient DiscoveryServiceClient discoveryServiceClient;

  // no-arg constructor required for serialization/deserialization to work
  @SuppressWarnings("unused")
  public InMemorySerializableServiceDiscoverer() {
    this.discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
  }

  public InMemorySerializableServiceDiscoverer(Id.Application application,
                                               DiscoveryServiceClient discoveryServiceClient) {
    super(application);
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public DiscoveryServiceClient getDiscoveryServiceClient() {
    return discoveryServiceClient;
  }
}
