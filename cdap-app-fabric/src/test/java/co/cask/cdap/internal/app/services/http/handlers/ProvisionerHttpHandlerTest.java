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
import co.cask.cdap.proto.provisioner.ProvisionerDetail;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import com.google.common.collect.ImmutableList;
import com.google.gson.reflect.TypeToken;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Unit tests for ProvisionerHttpHandler
 */
public class ProvisionerHttpHandlerTest extends AppFabricTestBase {
  private static final Type LIST_PROVISIONER_DETAIL = new TypeToken<List<ProvisionerDetail>>() { }.getType();

  @Test
  public void testListAndGetProvisioners() throws Exception {
    // in unit test, we only have the mock provisioner currently
    ProvisionerSpecification spec = new MockProvisioner().getSpec();
    ProvisionerDetail expected = new ProvisionerDetail(spec.getName(), spec.getDescription(), new ArrayList<>());
    List<ProvisionerDetail> details = listProvisioners();
    Assert.assertEquals(ImmutableList.of(expected), details);

    // get a non-existing provisioner should get a 404
    getProvisioner("nonExisting", 404);

    // get the mock provisioner
    Assert.assertEquals(expected, getProvisioner(MockProvisioner.NAME, 200));
  }

  private List<ProvisionerDetail> listProvisioners() throws Exception {
    HttpResponse response = doGet("/v3/provisioners");
    return GSON.fromJson(EntityUtils.toString(response.getEntity()), LIST_PROVISIONER_DETAIL);
  }

  @Nullable
  private ProvisionerDetail getProvisioner(String provisionerName, int expectedCode) throws Exception {
    HttpResponse response = doGet(String.format("/v3/provisioners/%s", provisionerName));
    Assert.assertEquals(expectedCode, response.getStatusLine().getStatusCode());
    if (expectedCode == HttpResponseStatus.OK.code()) {
      return GSON.fromJson(EntityUtils.toString(response.getEntity()), ProvisionerDetail.class);
    }
    return null;
  }
}
