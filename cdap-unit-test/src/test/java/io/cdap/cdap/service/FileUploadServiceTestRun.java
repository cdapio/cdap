/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.cdap.service;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.lib.PartitionDetail;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.base.TestFrameworkTestBase;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class FileUploadServiceTestRun extends TestFrameworkTestBase {

  @Test
  public void testFileUploadService() throws Exception {
    ApplicationManager appManager = deployApplication(FileUploadApp.class);

    // Start the service
    ServiceManager serviceManager = appManager.getServiceManager(FileUploadApp.SERVICE_NAME).start();
    serviceManager.waitForRun(ProgramRunStatus.RUNNING, 10, TimeUnit.SECONDS);
    try {
      // Upload URL is "base/upload/pfs/[partition_value], which the partition value is a long
      URI serviceURI = serviceManager.getServiceURL(10, TimeUnit.SECONDS).toURI();

      // Upload with wrong MD5, should get 400.
      byte[] content = Strings.repeat("0123456789 ", 100).getBytes(Charsets.UTF_8);
      Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST,
                          upload(serviceURI.resolve("upload/" + FileUploadApp.PFS_NAME + "/1").toURL(), content,
                                 "123", 30));

      long beforeUploadTime = System.currentTimeMillis();

      // Upload with right MD5, should get 200
      Assert.assertEquals(HttpURLConnection.HTTP_OK, upload(
        serviceURI.resolve("upload/" + FileUploadApp.PFS_NAME + "/1").toURL(), content,
        Base64.getEncoder().encodeToString(Hashing.md5().hashBytes(content).asBytes()), 20));

      // Inspect the partitioned file set and verify the content
      PartitionedFileSet pfs = (PartitionedFileSet) getDataset(FileUploadApp.PFS_NAME).get();
      PartitionDetail partition = pfs.getPartition(PartitionKey.builder().addLongField("time", 1).build());

      Assert.assertNotNull(partition);

      // Verify a notification should have been published for the new partition
      List<Notification> notifications = getDataNotifications(beforeUploadTime);

      // Should have one message
      Assert.assertEquals(1, notifications.size());
      verifyDataNotification(notifications.get(0),
                             NamespaceId.DEFAULT.dataset(FileUploadApp.PFS_NAME),
                             Collections.singletonList(PartitionKey.builder().addLongField("time", 1L).build()));

      // There should be one file under the partition directory
      List<Location> locations = partition.getLocation().list();
      Assert.assertEquals(1, locations.size());
      Assert.assertArrayEquals(content, ByteStreams.toByteArray(Locations.newInputSupplier(locations.get(0))));

      // Verify the tracking table of chunks sizes
      KeyValueTable trackingTable = (KeyValueTable) getDataset(FileUploadApp.KV_TABLE_NAME).get();
      CloseableIterator<KeyValue<byte[], byte[]>> iter = trackingTable.scan(null, null);
      // Sum up all chunks sizes as being tracked by the tracking table.
      long sum = 0;
      int iterSize = 0;
      while (iter.hasNext()) {
        KeyValue<byte[], byte[]> kv = iter.next();
        sum += Bytes.toInt(kv.getKey()) * Bytes.toLong(kv.getValue());
        iterSize++;
      }
      // The iterator should have size >= 2, since we uses different chunk size for two different upload
      Assert.assertTrue(iterSize >= 2);

      // The sum of all chunks sizes should be the same as the
      // content size * 2 (since we have one failure, one success upload)
      Assert.assertEquals(content.length * 2, sum);

    } finally {
      serviceManager.stop();
      serviceManager.waitForRun(ProgramRunStatus.KILLED, 10, TimeUnit.SECONDS);
    }
  }

  private int upload(URL url, byte[] content, String md5, int chunkSize) throws IOException, NoSuchAlgorithmException {
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    try {
      urlConn.setChunkedStreamingMode(chunkSize);
      urlConn.setDoOutput(true);
      urlConn.setRequestMethod("POST");
      urlConn.addRequestProperty("Content-MD5", md5);
      urlConn.getOutputStream().write(content);
      return urlConn.getResponseCode();
    } finally {
      urlConn.disconnect();
    }
  }
}
