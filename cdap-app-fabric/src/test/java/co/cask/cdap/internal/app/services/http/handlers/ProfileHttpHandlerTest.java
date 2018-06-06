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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.provision.MockProvisioner;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.profile.Profile;
import co.cask.cdap.proto.provisioner.ProvisionerDetail;
import co.cask.cdap.proto.provisioner.ProvisionerInfo;
import co.cask.cdap.proto.provisioner.ProvisionerPropertyValue;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.reflect.TypeToken;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Unit tests for profile http handler
 */
public class ProfileHttpHandlerTest extends AppFabricTestBase {
  private static final Type LIST_PROFILE = new TypeToken<List<Profile>>() { }.getType();
  private static final List<ProvisionerPropertyValue> PROPERTY_SUMMARIES =
    ImmutableList.<ProvisionerPropertyValue>builder()
      .add(new ProvisionerPropertyValue("1st property", "1st value", false))
      .add(new ProvisionerPropertyValue("2nd property", "2nd value", true))
      .add(new ProvisionerPropertyValue("3rd property", "3rd value", false))
      .build();

  @Test
  public void testListAndGetProfiles() throws Exception {
    // no profile should be there in default namespace
    List<Profile> profiles = listProfiles(NamespaceId.DEFAULT, false, 200);
    Assert.assertEquals(Collections.emptyList(), profiles);

    // try to list all profiles including system namespace before putting a new one, there should only exist a default
    // profile
    profiles = listProfiles(NamespaceId.DEFAULT, true, 200);
    Assert.assertEquals(Collections.singletonList(Profile.DEFAULT), profiles);

    // test get single profile endpoint
    Profile defaultProfile = getProfile(NamespaceId.SYSTEM, "default", 200);
    Assert.assertEquals(Profile.DEFAULT, defaultProfile);

    // get a nonexisting profile should get a not found code
    getProfile(NamespaceId.DEFAULT, "nonExisting", 404);
  }

  @Test
  public void testPutAndDeleteProfiles() throws Exception {
    Profile invalidProfile = new Profile("MyProfile", "my profile for testing",
                                         new ProvisionerInfo("nonExisting", PROPERTY_SUMMARIES));
    // adding a profile with non-existing provisioner should get a 400
    putProfile(NamespaceId.DEFAULT, invalidProfile.getName(), invalidProfile, 400);

    // put a profile with the mock provisioner
    Profile expected = new Profile("MyProfile", "my profile for testing",
                                   new ProvisionerInfo(MockProvisioner.NAME, PROPERTY_SUMMARIES));
    putProfile(NamespaceId.DEFAULT, expected.getName(), expected, 200);

    // get the profile
    Profile actual = getProfile(NamespaceId.DEFAULT, expected.getName(), 200);
    Assert.assertEquals(expected, actual);

    // list all profiles, should get 2 profiles
    List<Profile> profiles = listProfiles(NamespaceId.DEFAULT, true, 200);
    Set<Profile> expectedList = ImmutableSet.of(Profile.DEFAULT, expected);
    Assert.assertEquals(expectedList.size(), profiles.size());
    Assert.assertEquals(expectedList, new HashSet<>(profiles));

    // adding the same profile should still succeed
    putProfile(NamespaceId.DEFAULT, expected.getName(), expected, 200);

    // get non-existing profile should get a 404
    deleteProfile(NamespaceId.DEFAULT, "nonExisting", 404);

    // delete the profile
    deleteProfile(NamespaceId.DEFAULT, expected.getName(), 200);
    Assert.assertEquals(Collections.emptyList(), listProfiles(NamespaceId.DEFAULT, false, 200));

    // if given some unrelated json, it should return a 400 instead of 500
    ProvisionerSpecification spec = new MockProvisioner().getSpec();
    ProvisionerDetail test = new ProvisionerDetail(spec.getName(), spec.getLabel(), spec.getDescription(),
                                                   new ArrayList<>());
    putProfile(NamespaceId.DEFAULT, test.getName(), test, 400);
  }

  private List<Profile> listProfiles(NamespaceId namespace, boolean includeSystem, int expectedCode) throws Exception {
    HttpResponse response = doGet(String.format("/v3/namespaces/%s/profiles?includeSystem=%s",
                                                namespace.getNamespace(), includeSystem));
    Assert.assertEquals(expectedCode, response.getStatusLine().getStatusCode());
    if (expectedCode == HttpResponseStatus.OK.code()) {
      return GSON.fromJson(EntityUtils.toString(response.getEntity()), LIST_PROFILE);
    }
    return Collections.emptyList();
  }

  @Nullable
  private Profile getProfile(NamespaceId namespace, String profileName, int expectedCode) throws Exception {
    HttpResponse response = doGet(String.format("/v3/namespaces/%s/profiles/%s",
                                                namespace.getNamespace(), profileName));
    Assert.assertEquals(expectedCode, response.getStatusLine().getStatusCode());
    if (expectedCode == HttpResponseStatus.OK.code()) {
      return GSON.fromJson(EntityUtils.toString(response.getEntity()), Profile.class);
    }
    return null;
  }

  private void putProfile(NamespaceId namespace, String profileName, Object profile,
                          int expectedCode) throws Exception {
    HttpResponse response = doPut(String.format("/v3/namespaces/%s/profiles/%s",
                                                namespace.getNamespace(), profileName), GSON.toJson(profile));
    Assert.assertEquals(expectedCode, response.getStatusLine().getStatusCode());
  }

  private void deleteProfile(NamespaceId namespace, String profileName, int expectedCode) throws Exception {
    HttpResponse response = doDelete(String.format("/v3/namespaces/%s/profiles/%s",
                                                   namespace.getNamespace(), profileName));
    Assert.assertEquals(expectedCode, response.getStatusLine().getStatusCode());
  }
}
