/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.support.services;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.support.lib.SupportBundleFileNames;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Support bundle service tests.
 */
public class SupportBundleServiceTest extends AppFabricTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleService.class);
  private static final Gson GSON = new GsonBuilder().create();

  private static CConfiguration configuration;

  @BeforeClass
  public static void setup() {
    Injector injector = getInjector();
    configuration = injector.getInstance(CConfiguration.class);
  }

  @Test
  public void testDeleteOldBundle() throws Exception {
    File tempFolder = new File(configuration.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    createNamespace("default");
    String path = String.format("%s/support/bundle?namespace=default", Constants.Gateway.API_VERSION_3);
    List<String> bundleIdList = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      HttpResponse response = doPost(path);
      Assert.assertEquals(HttpResponseStatus.OK.code(), response.getResponseCode());
      String bundleId = response.getResponseBodyAsString();
      bundleIdList.add(bundleId);
    }

    File bundleFile = new File(tempFolder, bundleIdList.get(4));
    SupportBundleStatus supportBundleStatus =
      new SupportBundleStatus(bundleIdList.get(4), System.currentTimeMillis(), null, CollectionState.FAILED);
    try (FileWriter statusFile = new FileWriter(new File(bundleFile, SupportBundleFileNames.STATUS_FILE_NAME))) {
      GSON.toJson(supportBundleStatus, statusFile);
    } catch (Exception e) {
      LOG.error("Can not update status file ", e);
      Assert.fail();
    }
    //Exceed the maximum number of folder allows in bundle
    doPost(path);
    File[] bundleFiles =
      tempFolder.listFiles((dir, name) -> !name.startsWith(".") && !dir.isHidden() && dir.isDirectory());
    Assert.assertEquals(7, bundleFiles.length);
    File expectedDeletedBundle = new File(tempFolder.getPath(), bundleIdList.get(4));
    Assert.assertFalse(expectedDeletedBundle.exists());
  }
}
