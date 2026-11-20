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

package io.cdap.cdap.data2.dataset2.lib.kv;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.util.Collections;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link LevelDBKVTableDefinition}.
 */
public class LevelDBKVTableDefinitionTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testPutGetDeleteAndGetTableIoExceptionPropagation() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_DATA_LEVELDB_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    try (LevelDBTableService realService = new LevelDBTableService()) {
      realService.setConfiguration(cConf);

      LevelDBKVTableDefinition def = new LevelDBKVTableDefinition("levelDbKv");
      Injector injector = Guice.createInjector(new AbstractModule() {
        @Override
        protected void configure() {
          bind(CConfiguration.class).toInstance(cConf);
          bind(LevelDBTableService.class).toInstance(realService);
        }
      });
      injector.injectMembers(def);

      DatasetContext context = DatasetContext.from(NamespaceId.DEFAULT.getNamespace());
      DatasetSpecification spec = def.configure("kv1", DatasetProperties.EMPTY);

      DatasetAdmin admin = def.getAdmin(context, spec, null);
      Assert.assertFalse(admin.exists());
      admin.create();
      Assert.assertTrue(admin.exists());

      try (NoTxKeyValueTable table = def.getDataset(context, spec, Collections.emptyMap(), null)) {
        byte[] key = Bytes.toBytes("k1");
        byte[] val = Bytes.toBytes("v1");
        table.put(key, val);
        Assert.assertArrayEquals(val, table.get(key));
        table.put(key, null);
        Assert.assertNull(table.get(key));
      }
      admin.drop();
    }

    // Verify getTable() propagates IOException from LevelDBTableService as RuntimeException(IOException)
    final IOException expectedIo = new IOException("leveldb open failed");
    LevelDBTableService failingService = Mockito.mock(LevelDBTableService.class);
    Mockito.when(failingService.getTable(Mockito.anyString())).thenThrow(expectedIo);

    LevelDBKVTableDefinition failingDef = new LevelDBKVTableDefinition("levelDbKv");
    Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(CConfiguration.class).toInstance(cConf);
        bind(LevelDBTableService.class).toInstance(failingService);
      }
    }).injectMembers(failingDef);

    DatasetContext context = DatasetContext.from(NamespaceId.DEFAULT.getNamespace());
    DatasetSpecification spec = failingDef.configure("kvFail", DatasetProperties.EMPTY);
    try (NoTxKeyValueTable table =
        failingDef.getDataset(context, spec, Collections.emptyMap(), null)) {
      try {
        table.get(Bytes.toBytes("k1"));
        Assert.fail("Expected RuntimeException wrapping IOException");
      } catch (RuntimeException actual) {
        Assert.assertSame(expectedIo, actual.getCause());
      }
    }
  }
}
