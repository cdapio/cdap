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

package co.cask.cdap.app.guice;

import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.client.ProgramDiscoveryExploreClient;
import co.cask.cdap.notifications.feeds.client.NotificationFeedClientModule;
import co.cask.cdap.security.impersonation.DefaultOwnerAdmin;
import co.cask.cdap.security.impersonation.OwnerAdmin;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
 * Guice module for accessing stream in program runtime container.
 * This module should be removed when stream is removed.
 */
public class DistributedProgramStreamModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);

    install(new ViewAdminModules().getDistributedModules());
    install(new StreamAdminModules().getDistributedModules());
    install(new NotificationFeedClientModule());
    install(new AbstractModule() {
      @Override
      protected void configure() {
        // bind explore client to ProgramDiscoveryExploreClient which is aware of the programId
        bind(ExploreClient.class).to(ProgramDiscoveryExploreClient.class).in(Scopes.SINGLETON);
      }
    });
  }
}
