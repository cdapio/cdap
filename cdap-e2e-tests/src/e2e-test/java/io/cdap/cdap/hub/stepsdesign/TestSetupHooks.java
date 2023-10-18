/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.hub.stepsdesign;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.StorageClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

/**
 * Setup gcs for Hub tests.
 */
public class TestSetupHooks {
  public static String gcsTargetBucketName = StringUtils.EMPTY;
  public static String gcsSourceBucketName = StringUtils.EMPTY;

  @Before(order = 1, value = "@GCS_SOURCE_TEST")
  public static void createBucketWithCSVFile() throws IOException, URISyntaxException {
    gcsSourceBucketName = createGCSBucketWithFile(PluginPropertyUtils.pluginProp("testFile"));
    PluginPropertyUtils.addPluginProp("gcsSourceBucket", "gs://" + gcsSourceBucketName + "/" +
      PluginPropertyUtils.pluginProp("testFile"));
    BeforeActions.scenario.write("GCS source bucket name - " + gcsSourceBucketName);
  }

  private static String createGCSBucketWithFile(String filePath)
    throws IOException, URISyntaxException {
    String bucketName = StorageClient.createBucket("e2e-test-" + UUID.randomUUID()).getName();
    StorageClient.uploadObject(bucketName, filePath, filePath);
    return bucketName;
  }

  @After(order = 1, value = "@GCS_SOURCE_TEST")
  public static void deleteSourceBucketWithFile() {
    deleteGCSBucket(gcsSourceBucketName);
  }

  private static void deleteGCSBucket(String bucketName) {
    try {
      for (Blob blob : StorageClient.listObjects(bucketName).iterateAll()) {
        StorageClient.deleteObject(bucketName, blob.getName());
      }
      StorageClient.deleteBucket(bucketName);
      BeforeActions.scenario.write("Deleted GCS Bucket " + bucketName);
    } catch (StorageException | IOException e) {
      if (e.getMessage().contains("The specified bucket does not exist")) {
        BeforeActions.scenario.write("GCS Bucket " + bucketName + " does not exist.");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }

  @Before(order = 1, value = "@GCS_SINK_TEST")
  public static void setTempTargetGCSBucketName() {
    gcsTargetBucketName = "cdf-e2e-test-" + UUID.randomUUID();
    PluginPropertyUtils.addPluginProp("gcsTargetBucketName", gcsTargetBucketName);
    PluginPropertyUtils.addPluginProp("gcsTargetPath", "gs://" + gcsTargetBucketName);
    BeforeActions.scenario.write("GCS target bucket name - " + gcsTargetBucketName);
  }

  @After(order = 1, value = "@GCS_SINK_TEST")
  public static void deleteTargetBucketWithFile() {
    deleteGCSBucket(gcsTargetBucketName);
    PluginPropertyUtils.removePluginProp("gcsTargetBucketName");
    PluginPropertyUtils.removePluginProp("gcsTargetPath");
  }
}
