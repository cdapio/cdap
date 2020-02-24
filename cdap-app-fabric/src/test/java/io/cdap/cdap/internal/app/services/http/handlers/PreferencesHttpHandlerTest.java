/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.gateway.handlers.PreferencesHttpHandler;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.PreferencesMetadata;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.profile.Profile;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link PreferencesHttpHandler}
 */
public class PreferencesHttpHandlerTest extends AppFabricTestBase {

  private static Store store;

  @BeforeClass
  public static void init() {
    store = getInjector().getInstance(Store.class);
  }

  private void addApplication(String namespace, Application app) {
    ApplicationSpecification appSpec = Specifications.from(app);
    store.addApplication(new ApplicationId(namespace, appSpec.getName()), appSpec);
  }

  @Test
  public void testInstance() throws Exception {
    String uri = getPreferenceURI();

    // Create preferences
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getPreferences(uri, false, 200));
    Assert.assertEquals(propMap, getPreferences(uri, true, 200));
    propMap.put("k1", "3@#3");
    propMap.put("@#$#ljfds", "231@#$");
    setPreferences(uri, propMap, 200);

    // Check the metadata of preferences
    PreferencesMetadata metadata1;
    metadata1 = getPreferencesMetadata(uri, HttpResponseStatus.OK);
    Assert.assertNotNull(metadata1);
    Assert.assertTrue(metadata1.getSeqId() > 0);

    // Update preferences
    propMap.put("more_key", "more_val");
    setPreferences(uri, propMap, 200);
    Assert.assertEquals(propMap, getPreferences(uri, false, 200));
    Assert.assertEquals(propMap, getPreferences(uri, true, 200));

    // Check the metadata of update preferences
    PreferencesMetadata metadata2;
    metadata2 = getPreferencesMetadata(uri, HttpResponseStatus.OK);
    Assert.assertNotNull(metadata2);
    Assert.assertTrue(metadata2.getSeqId() > metadata1.getSeqId());

    // Delete preferences
    propMap.clear();
    deletePreferences(uri, 200);
    Assert.assertEquals(propMap, getPreferences(uri, false, 200));
    Assert.assertEquals(propMap, getPreferences(uri, true, 200));

    // Deleting preferences just set preferences to empty, the row and metadata still exist.
    PreferencesMetadata metadata3 = getPreferencesMetadata(uri, HttpResponseStatus.OK);
    Assert.assertNotNull(metadata3);
    Assert.assertTrue(metadata3.getSeqId() > metadata2.getSeqId());
  }

  @Test
  public void testNamespace() throws Exception {
    String uri = getPreferenceURI();
    String uriNamespace1 = getPreferenceURI(TEST_NAMESPACE1);
    String uriNamespace2 = getPreferenceURI(TEST_NAMESPACE2);
    PreferencesMetadata metadata1, metadata2;

    // Verify preferences are empty
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getPreferences(uriNamespace1, false, 200));
    Assert.assertEquals(propMap, getPreferences(uriNamespace2, false, 200));
    Assert.assertEquals(propMap, getPreferences(uriNamespace1, true, 200));
    Assert.assertEquals(propMap, getPreferences(uriNamespace2, true, 200));

    // Set preferences for namespace1 and verify
    propMap.put("k1", "3@#3");
    propMap.put("@#$#ljfds", "231@#$");
    setPreferences(uriNamespace1, propMap, 200);
    Assert.assertEquals(propMap, getPreferences(uriNamespace1, false, 200));
    Assert.assertEquals(propMap, getPreferences(uriNamespace1, true, 200));

    // Check the metadata of preferences in namespace1
    metadata1 = getPreferencesMetadata(uriNamespace1, HttpResponseStatus.OK);
    Assert.assertNotNull(metadata1);
    Assert.assertTrue(metadata1.getSeqId() > 0);

    // Set preferences on instance. Preferences for namespace1 unaffected, but affect the resolved for namespace2
    Map<String, String> instanceMap = Maps.newHashMap();
    instanceMap.put("k1", "432432*#######");
    setPreferences(uri, instanceMap, 200);
    Assert.assertEquals(instanceMap, getPreferences(uri, true, 200));
    Assert.assertEquals(instanceMap, getPreferences(uriNamespace2, true, 200));
    Assert.assertEquals(propMap, getPreferences(uriNamespace1, true, 200));

    // Check the metadata of preferences in namespace1 is unchanged.
    Assert.assertEquals(metadata1, getPreferencesMetadata(uriNamespace1, HttpResponseStatus.OK));

    // Update preferences on instance. Check the resolved for namespace2 reflects the change.
    instanceMap.put("k2", "(93424");
    setPreferences(uri, instanceMap, 200);
    instanceMap.putAll(propMap);
    Assert.assertEquals(instanceMap, getPreferences(uriNamespace1, true, 200));

    // Delete preferences for both namespace1 and namespace2
    deletePreferences(uriNamespace1, 200);
    deletePreferences(uriNamespace2, 200);

    // Set new preferences on the instance and check that shows up in the resolved for both namespaces.
    instanceMap.clear();
    instanceMap.put("*&$kjh", "*(&*1");
    setPreferences(uri, instanceMap, 200);
    Assert.assertEquals(instanceMap, getPreferences(uriNamespace2, true, 200));
    Assert.assertEquals(instanceMap, getPreferences(uriNamespace1, true, 200));
    instanceMap.clear();
    Assert.assertEquals(instanceMap, getPreferences(uriNamespace2, false, 200));
    Assert.assertEquals(instanceMap, getPreferences(uriNamespace1, false, 200));

    // Delete preferences for the instance.
    deletePreferences(uri, 200);
    Assert.assertEquals(instanceMap, getPreferences(uriNamespace2, true, 200));
    Assert.assertEquals(instanceMap, getPreferences(uriNamespace1, true, 200));
    getPreferences(getPreferenceURI("invalidNamespace"), true, 404);
  }

  @Test
  public void testApplication() throws Exception {
    String appName = AllProgramsApp.NAME;
    String namespace1 = TEST_NAMESPACE1;
    String uri = getPreferenceURI();
    String uriNamespace1 = getPreferenceURI(namespace1);
    String uriApp = getPreferenceURI(namespace1, appName);
    PreferencesMetadata metadata, newMetadata;

    // Application not created yet.
    metadata = getPreferencesMetadata(getPreferenceURI(namespace1, "some_non_existing_app"),
                                      HttpResponseStatus.NOT_FOUND);
    Assert.assertNull(metadata);

    // Create the app.
    addApplication(namespace1, new AllProgramsApp());
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getPreferences(uriApp, false, 200));
    Assert.assertEquals(propMap, getPreferences(uriApp, true, 200));
    getPreferences(getPreferenceURI(namespace1, "InvalidAppName"), false, 404);

    // Application created but no preferences created yet, so no metadata.
    metadata = getPreferencesMetadata(uriApp, HttpResponseStatus.OK);
    Assert.assertNull(metadata);

    // Set the preference
    setPreferences(uri, ImmutableMap.of("k1", "instance"), 200);

    setPreferences(uriNamespace1, ImmutableMap.of("k1", "namespace"), 200);
    setPreferences(uriApp, ImmutableMap.of("k1", "application"), 200);
    Assert.assertEquals("application",
                        getPreferences(uriApp, false, 200).get("k1"));
    Assert.assertEquals("application",
                        getPreferences(uriApp, true, 200).get("k1"));
    Assert.assertEquals("namespace", getPreferences(uriNamespace1, false, 200).get("k1"));
    Assert.assertEquals("namespace", getPreferences(uriNamespace1, true, 200).get("k1"));
    Assert.assertEquals("instance", getPreferences(uri, true, 200).get("k1"));
    Assert.assertEquals("instance", getPreferences(uri, false, 200).get("k1"));

    // Application created but no preferences created yet, so no metadata.
    metadata = getPreferencesMetadata(uriApp, HttpResponseStatus.OK);
    Assert.assertNotNull(metadata);
    Assert.assertTrue(metadata.getSeqId() > 0);

    // Delete preferences on the application.
    deletePreferences(uriApp, 200);
    Assert.assertEquals("namespace",
                        getPreferences(uriApp, true, 200).get("k1"));
    Assert.assertNull(getPreferences(uriApp, false, 200).get("k1"));

    // Even if preferences have been deleted, metadata still remains
    newMetadata = getPreferencesMetadata(uriApp, HttpResponseStatus.OK);
    Assert.assertNotNull(newMetadata);
    Assert.assertTrue(newMetadata.getSeqId() > metadata.getSeqId());

    // Delete preferences on the namespace and verify.
    deletePreferences(uriNamespace1, 200);
    Assert.assertEquals("instance",
                        getPreferences(uriApp, true, 200).get("k1"));
    Assert.assertEquals("instance",
                        getPreferences(uriNamespace1, true, 200).get("k1"));
    Assert.assertNull(getPreferences(uriNamespace1, false, 200).get("k1"));

    // Delete preferences on the instance and verify.
    deletePreferences(uri, 200);
    Assert.assertNull(getPreferences(uri, true, 200).get("k1"));
    Assert.assertNull(getPreferences(uriNamespace1, true, 200).get("k1"));
    Assert.assertNull(getPreferences(uriApp, true, 200).get("k1"));
  }

  @Test
  public void testProgram() throws Exception {
    String uri = getPreferenceURI();
    String namespace2 = TEST_NAMESPACE2;
    String appName = AllProgramsApp.NAME;
    String uriNamespace2Service = getPreferenceURI(namespace2, appName, "services", AllProgramsApp.NoOpService.NAME);
    PreferencesMetadata metadata, newMetadata;

    // Create application.
    addApplication(namespace2, new AllProgramsApp());
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getPreferences(uriNamespace2Service, false, 200));
    getPreferences(getPreferenceURI(
      namespace2, appName, "invalidType", "somename"), false, 400);
    getPreferences(getPreferenceURI(
      namespace2, appName, "services", "somename"), false, 404);

    // No preferences have been set yet.
    metadata = getPreferencesMetadata(uriNamespace2Service, HttpResponseStatus.OK);
    Assert.assertNull(metadata);

    // Set preferences for the program
    propMap.put("k1", "k349*&#$");
    setPreferences(uriNamespace2Service, propMap, 200);
    Assert.assertEquals(propMap, getPreferences(
      uriNamespace2Service, false, 200));

    // Check the metadata of preferences
    metadata = getPreferencesMetadata(uriNamespace2Service, HttpResponseStatus.OK);
    Assert.assertNotNull(metadata);
    Assert.assertTrue(metadata.getSeqId() > 0);

    // Set preferences on the instance and verify.
    propMap.put("k1", "instance");
    setPreferences(uri, propMap, 200);
    Assert.assertEquals(propMap, getPreferences(uri, true, 200));
    propMap.put("k1", "k349*&#$");
    Assert.assertEquals(propMap, getPreferences(
      uriNamespace2Service, false, 200));

    // Delete preferences on the program and the instance
    deletePreferences(uriNamespace2Service, 200);
    propMap.put("k1", "instance");
    Assert.assertEquals(0, getPreferences(uriNamespace2Service, false, 200).size());
    Assert.assertEquals(propMap, getPreferences(uriNamespace2Service, true, 200));
    deletePreferences(uri, 200);
    propMap.clear();
    Assert.assertEquals(propMap, getPreferences(
      uriNamespace2Service, false, 200));
    Assert.assertEquals(propMap, getPreferences(uri, false, 200));
  }

  @Test
  public void testSetPreferenceWithProfiles() throws Exception {
    // put my profile
    ProfileId myProfile = new ProfileId(TEST_NAMESPACE1, "MyProfile");
    putProfile(myProfile, Profile.NATIVE, 200);

    // put some properties with my profile, it should work fine
    Map<String, String> properties = new HashMap<>();
    properties.put("1st key", "1st value");
    properties.put("2nd key", "2nd value");
    properties.put(SystemArguments.PROFILE_NAME, "USER:MyProfile");
    Map<String, String> expected = ImmutableMap.copyOf(properties);
    setPreferences(getPreferenceURI(TEST_NAMESPACE1), properties, 200);
    Assert.assertEquals(expected, getPreferences(getPreferenceURI(TEST_NAMESPACE1), false, 200));

    // put some property with non-existing profile, it should fail with 404
    properties.put(SystemArguments.PROFILE_NAME, "NonExisting");
    setPreferences(getPreferenceURI(TEST_NAMESPACE1), properties, 404);
    Assert.assertEquals(expected, getPreferences(getPreferenceURI(TEST_NAMESPACE1), false, 200));

    // disable the profile and put again, it should fail with 409
    disableProfile(myProfile, 200);
    properties.put(SystemArguments.PROFILE_NAME, "USER:MyProfile");
    setPreferences(getPreferenceURI(TEST_NAMESPACE1), properties, 409);
    Assert.assertEquals(expected, getPreferences(getPreferenceURI(TEST_NAMESPACE1), false, 200));

    deletePreferences(getPreferenceURI(TEST_NAMESPACE1), 200);
  }
}
