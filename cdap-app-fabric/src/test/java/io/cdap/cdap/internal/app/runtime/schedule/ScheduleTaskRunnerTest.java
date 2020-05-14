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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import io.cdap.cdap.internal.app.services.PropertiesResolver;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.NoOpOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Tests {@link ScheduleTaskRunner}.
 */
public class ScheduleTaskRunnerTest {

  @Test
  public void testRuntimeArgumentResolution() throws Exception {
    Injector injector = Guice.createInjector(
      new ConfigModule(),
      binder -> {
        binder.bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);
        binder.bind(NamespaceAdmin.class).to(InMemoryNamespaceAdmin.class);
        binder.bind(NamespaceQueryAdmin.class).to(InMemoryNamespaceAdmin.class);
        binder.bind(PreferencesFetcher.class)
          .toInstance(new FakePreferencesFetcher(Collections.singletonMap("key", "should-be-overridden")));
      });

    PropertiesResolver propertiesResolver = injector.getInstance(PropertiesResolver.class);

    ApplicationId appId = NamespaceId.DEFAULT.app("app");
    ProgramSchedule programSchedule = new ProgramSchedule(
      "schedule", "desc", appId.workflow("wf2"),
      Collections.singletonMap("key", "val"), new ProgramStatusTrigger(appId.workflow("wf1")),
      Collections.emptyList());
    Map<String, String> userArgs = ScheduleTaskRunner.getUserArgs(programSchedule, propertiesResolver);
    Assert.assertEquals("val", userArgs.get("key"));
  }

  /**
   * Fake preferences that just returns whatever it is configured to return.
   */
  private static class FakePreferencesFetcher implements PreferencesFetcher {
    private final Map<String, String> properties;

    private FakePreferencesFetcher(Map<String, String> properties) {
      this.properties = properties;
    }

    @Override
    public PreferencesDetail get(EntityId entityId, boolean resolved) {
      return new PreferencesDetail(properties, 0L, resolved);
    }
  }
}
