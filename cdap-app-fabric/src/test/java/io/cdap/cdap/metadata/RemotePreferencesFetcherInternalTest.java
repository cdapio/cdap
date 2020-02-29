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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.InstanceId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link RemotePreferencesFetcherInternal}
 */
public class RemotePreferencesFetcherInternalTest extends AppFabricTestBase {
  private static PreferencesFetcher fetcher = null;

  @BeforeClass
  public static void init() {
    fetcher = getInjector().getInstance(RemotePreferencesFetcherInternal.class);
  }

  @Test
  public void testGetPreferences() throws Exception {
    PreferencesDetail preferences = null;
    EntityId entityId = null;

    // Get preferences on instance, but none was set.
    entityId = new InstanceId("");
    preferences = fetcher.get(entityId, false);
    Assert.assertEquals(Collections.emptyMap(), preferences.getProperties());
    Assert.assertFalse(preferences.getResolved());
    // SeqId should be 0 when preferences never get set on the entity.
    Assert.assertEquals(0, preferences.getSeqId());

    // Set preferences on instance and fetch again.
    Map<String, String> instanceProperties = ImmutableMap.of("instance-key1", "instance-val1");
    setPreferences(getPreferenceURI(), instanceProperties, 200);
    preferences = fetcher.get(entityId, false);
    Assert.assertEquals(instanceProperties, preferences.getProperties());
    Assert.assertFalse(preferences.getResolved());
    Assert.assertTrue(preferences.getSeqId() > 0);

    // Deploy the application.
    String namespace = TEST_NAMESPACE1;
    String appName = AllProgramsApp.NAME;
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    // Get preferences on the application, but none was set.
    entityId = new ApplicationId(namespace, appName);
    preferences = fetcher.get(entityId, false);
    Assert.assertEquals(Collections.emptyMap(), preferences.getProperties());
    Assert.assertFalse(preferences.getResolved());
    Assert.assertEquals(0, preferences.getSeqId());

    // Get resolved preferences on the application, preferences on instance should be returned.
    entityId = new ApplicationId(namespace, appName);
    preferences = fetcher.get(entityId, true);
    Assert.assertEquals(instanceProperties, preferences.getProperties());
    Assert.assertTrue(preferences.getResolved());
    Assert.assertTrue(preferences.getSeqId() > 0);

    // Set preferences on application and fetch again, resolved preferences should be returned.
    Map<String, String> appProperties = ImmutableMap.of("app-key1", "app-val1");
    setPreferences(getPreferenceURI(namespace, appName), appProperties, 200);
    preferences = fetcher.get(entityId, true);
    Map<String, String> resolvedProperites = new HashMap<>();
    resolvedProperites.putAll(instanceProperties);
    resolvedProperites.putAll(appProperties);
    Assert.assertEquals(resolvedProperites, preferences.getProperties());
    Assert.assertTrue(preferences.getResolved());
    Assert.assertTrue(preferences.getSeqId() > 0);

    // Delete the app
    Assert.assertEquals(
      200,
      doDelete(getVersionedAPIPath("apps/",
                                   Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getResponseCode());
  }
}

