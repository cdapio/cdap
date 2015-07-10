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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetDefinition;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionExecutorFactory;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class DatasetUpgraderTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static DatasetUpgrader upgrader;
  private static DatasetFramework framework;

  @BeforeClass
  public static void setup() throws Exception {

    framework = dsFrameworkUtil.getFramework();
    Injector injector = dsFrameworkUtil.getInjector();
    upgrader = new DatasetUpgrader(dsFrameworkUtil.getConfiguration(), null,
                                   injector.getInstance(LocationFactory.class),
                                   injector.getInstance(NamespacedLocationFactory.class),
                                   null, framework, new TransactionExecutorFactory() {
      @Override
      public TransactionExecutor createExecutor(Iterable<TransactionAware> txAwares) {
        return dsFrameworkUtil.newTransactionExecutor(Iterables.toArray(txAwares, TransactionAware.class));
      }
    });
  }

  static final Id.DatasetInstance FS1 = Id.DatasetInstance.from("test", "fs1");
  static final Id.DatasetInstance FS2 = Id.DatasetInstance.from("dummy", "fs2");
  static final Id.DatasetInstance FS3 = Id.DatasetInstance.from("default", "fs3");
  static final Id.DatasetInstance FS4 = Id.DatasetInstance.from("dummy", "fs4");
  static final Id.DatasetInstance PFS1 = Id.DatasetInstance.from("test", "pfs1");
  static final Id.DatasetInstance PFS2 = Id.DatasetInstance.from("default", "pfs2");
  static final Id.DatasetInstance TPFS1 = Id.DatasetInstance.from("test", "tpfs1");
  static final Id.DatasetInstance TPFS2 = Id.DatasetInstance.from("test", "tpfs2");

  @Test
  public void testFileSetUpgrade() throws Exception {
    framework.createNamespace(Id.Namespace.from("test"));
    framework.createNamespace(Id.Namespace.from("dummy"));

    framework.addInstance(FileSet.class.getName(), FS1,
                          FileSetProperties.builder().setBasePath("/temp/fs1").build());
    DatasetSpecification spec = framework.getDatasetSpec(FS1);
    Assert.assertTrue(FileSetProperties.getBasePath(spec.getProperties()).startsWith("/"));
    Assert.assertNotNull(upgrader.containsFileSetWithAbsolutePath(spec));
    DatasetSpecification newSpec = upgrader.migrateDatasetSpec(spec);
    Assert.assertFalse(FileSetProperties.getBasePath(newSpec.getProperties()).startsWith("/"));
    Assert.assertNull(upgrader.containsFileSetWithAbsolutePath(newSpec));

    framework.addInstance(FileSet.class.getName(), FS2,
                          FileSetProperties.builder().setBasePath("temp/fs2").build());
    spec = framework.getDatasetSpec(FS2);
    Assert.assertNull(upgrader.containsFileSetWithAbsolutePath(spec));

    framework.addInstance("fileSet", FS3,
                          FileSetProperties.builder().setBasePath("//temp/fs3").build());
    spec = framework.getDatasetSpec(FS1);
    Assert.assertTrue(FileSetProperties.getBasePath(spec.getProperties()).startsWith("/"));
    Assert.assertNotNull(upgrader.containsFileSetWithAbsolutePath(spec));
    newSpec = upgrader.migrateDatasetSpec(spec);
    Assert.assertFalse(FileSetProperties.getBasePath(newSpec.getProperties()).startsWith("/"));
    Assert.assertNull(upgrader.containsFileSetWithAbsolutePath(newSpec));

    framework.addInstance("fileSet", FS4,
                          FileSetProperties.builder().setBasePath("temp/fs4").build());
    spec = framework.getDatasetSpec(FS2);
    Assert.assertNull(upgrader.containsFileSetWithAbsolutePath(spec));

    Partitioning partitioning = Partitioning.builder().addField("x", Partitioning.FieldType.STRING).build();

    framework.addInstance(PartitionedFileSet.class.getName(), PFS1,
                          PartitionedFileSetProperties.builder()
                            .setPartitioning(partitioning)
                            .setBasePath("/temp/pfs1").build());
    spec = framework.getDatasetSpec(PFS1);
    Assert.assertTrue(FileSetProperties.getBasePath(spec.getProperties()).startsWith("/"));
    Assert.assertTrue(FileSetProperties.getBasePath(spec.getSpecification(PartitionedFileSetDefinition.FILESET_NAME)
                                                      .getProperties()).startsWith("/"));
    Assert.assertNotNull(upgrader.containsFileSetWithAbsolutePath(spec));
    newSpec = upgrader.migrateDatasetSpec(spec);
    Assert.assertEquals(spec.getSpecification(PartitionedFileSetDefinition.PARTITION_TABLE_NAME),
                        newSpec.getSpecification(PartitionedFileSetDefinition.PARTITION_TABLE_NAME));
    Assert.assertFalse(FileSetProperties.getBasePath(newSpec.getProperties()).startsWith("/"));
    Assert.assertFalse(FileSetProperties.getBasePath(newSpec.getSpecification(PartitionedFileSetDefinition.FILESET_NAME)
                                                       .getProperties()).startsWith("/"));
    Assert.assertNull(upgrader.containsFileSetWithAbsolutePath(newSpec));

    framework.addInstance(PartitionedFileSet.class.getName(), PFS2,
                          PartitionedFileSetProperties.builder()
                            .setPartitioning(partitioning)
                            .setBasePath("temp/pfs2").build());
    spec = framework.getDatasetSpec(PFS2);
    Assert.assertNull(upgrader.containsFileSetWithAbsolutePath(spec));

    framework.addInstance(PartitionedFileSet.class.getName(), TPFS1,
                          FileSetProperties.builder().setBasePath("///temp/tpfs1").build());
    spec = framework.getDatasetSpec(TPFS1);
    Assert.assertTrue(FileSetProperties.getBasePath(spec.getProperties()).startsWith("/"));
    Assert.assertTrue(FileSetProperties.getBasePath(spec.getSpecification(PartitionedFileSetDefinition.FILESET_NAME)
                                                      .getProperties()).startsWith("/"));
    Assert.assertNotNull(upgrader.containsFileSetWithAbsolutePath(spec));
    newSpec = upgrader.migrateDatasetSpec(spec);
    Assert.assertEquals(spec.getSpecification(PartitionedFileSetDefinition.PARTITION_TABLE_NAME),
                        newSpec.getSpecification(PartitionedFileSetDefinition.PARTITION_TABLE_NAME));
    Assert.assertFalse(FileSetProperties.getBasePath(newSpec.getProperties()).startsWith("/"));
    Assert.assertFalse(FileSetProperties.getBasePath(newSpec.getSpecification(PartitionedFileSetDefinition.FILESET_NAME)
                                                       .getProperties()).startsWith("/"));
    Assert.assertNull(upgrader.containsFileSetWithAbsolutePath(newSpec));

    framework.addInstance(PartitionedFileSet.class.getName(), TPFS2,
                          FileSetProperties.builder().setBasePath("temp/tpfs2").build());
    spec = framework.getDatasetSpec(TPFS2);
    Assert.assertNull(upgrader.containsFileSetWithAbsolutePath(spec));
  }

}
