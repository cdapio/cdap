/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.read;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.framework.CDAPLogAppender;
import co.cask.cdap.logging.framework.LogPathIdentifier;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.logging.write.FileMetaDataWriter;
import co.cask.cdap.logging.write.LogLocation;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

public class FileMetadataTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static Injector injector;
  private static TransactionManager txManager;

  @BeforeClass
  public static void setUpContext() throws Exception {
    Configuration hConf = HBaseConfiguration.create();
    final CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    String logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR) + "/" + CDAPLogAppender.class.getSimpleName();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);

    injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new NonCustomLocationUnitTestModule().getModule(),
      new TransactionModules().getInMemoryModules(),
      new LoggingModules().getInMemoryModules(),
      new DataSetsModules().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getNoOpModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
          bind(NamespaceQueryAdmin.class).to(SimpleNamespaceQueryAdmin.class);
        }
      }
    );

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    txManager.stopAndWait();
  }

  @Test
  public void testFileMetadataReadWrite() throws Exception {
    FileMetaDataWriter fileMetaDataWriter = injector.getInstance(FileMetaDataWriter.class);
    LogPathIdentifier logPathIdentifier =
      new LogPathIdentifier(NamespaceId.DEFAULT.getNamespace(), "testApp", "testFlow");
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    Location location = locationFactory.create(TMP_FOLDER.newFolder().getPath()).append("/logs");
    long currentTime = System.currentTimeMillis();
    for (int i = 10; i <= 100; i += 10) {
      // i is the event time
      fileMetaDataWriter.writeMetaData(logPathIdentifier, i, currentTime,
                                       location.append(Integer.toString(i)));
    }

    // for the timestamp 80, add new new log path id with different current time.

    fileMetaDataWriter.writeMetaData(logPathIdentifier, 80, currentTime + 1,
                                     location.append("81"));

    fileMetaDataWriter.writeMetaData(logPathIdentifier, 80, currentTime + 2,
                                     location.append("82"));

    // reader test

    FileMetadataReader fileMetadataReader = injector.getInstance(FileMetadataReader.class);

    Assert.assertEquals(12, fileMetadataReader.listFiles(logPathIdentifier, 0, 100).size());
    Assert.assertEquals(5, fileMetadataReader.listFiles(logPathIdentifier, 20, 50).size());
    Assert.assertEquals(2, fileMetadataReader.listFiles(logPathIdentifier, 100, 150).size());
    // should include three files with event start time 80.
    List<LogLocation> locationList = fileMetadataReader.listFiles(logPathIdentifier, 81, 85);
    Assert.assertEquals(3, locationList.size());
    int count = 0;
    int eventTime = 80;
    for (LogLocation logLocation : locationList) {
      Assert.assertEquals(80, logLocation.getEventTimeMs());
      Assert.assertEquals(currentTime + count, logLocation.getFileCreationTimeMs());
      Assert.assertEquals(location.append(Long.toString(eventTime + count)), logLocation.getLocation());
      count++;
    }

    Assert.assertEquals(1, fileMetadataReader.listFiles(logPathIdentifier, 150, 1000).size());
  }
}
