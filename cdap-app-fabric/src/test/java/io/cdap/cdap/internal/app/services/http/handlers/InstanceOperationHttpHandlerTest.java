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

package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.io.Files;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

/**
 * Unit test for the {@link InstanceOperationHttpHandlerTest}.
 */
public class InstanceOperationHttpHandlerTest extends AppFabricTestBase {

  @Test
  public void testExportApps() throws Exception {
    List<String> namespaces = Arrays.asList("ns1", "ns2", "ns3");

    for (String ns : namespaces) {
      createNamespace(ns);
      deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, ns);
    }

    HttpResponse response = doGet(Constants.Gateway.API_VERSION_3 + "/export/apps");
    Assert.assertEquals(200, response.getResponseCode());

    // Validate the message digest of the content
    String digest = response.getHeaders().get("digest").stream().findFirst().orElse(null);
    Assert.assertNotNull(digest);
    String[] splits = digest.split("=", 2);
    MessageDigest md = MessageDigest.getInstance(splits[0]);
    Assert.assertEquals(splits[1], Base64.getEncoder().encodeToString(md.digest(response.getResponseBody())));

    File file = tmpFolder.newFile();
    file.delete();
    Files.write(response.getResponseBody(), file);

    File dir = tmpFolder.newFolder();
    BundleJarUtil.unJar(file, dir);

    for (String ns : namespaces) {
      File nsDir = new File(dir, ns);
      Assert.assertTrue(nsDir.isDirectory());
      Assert.assertTrue(new File(nsDir, "App.json").isFile());
    }
  }
}
