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

package co.cask.cdap.explore.guice;

import co.cask.cdap.explore.client.DiscoveryExploreClient;
import co.cask.cdap.explore.client.ExploreClient;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

/**
 * Explore client Guice module.
 */
public class ExploreClientModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(ExploreClient.class).to(DiscoveryExploreClient.class).in(Scopes.SINGLETON);
    expose(ExploreClient.class);
  }
}
