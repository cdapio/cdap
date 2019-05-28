/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.artifact;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactMeta;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactStore;
import io.cdap.cdap.internal.app.runtime.artifact.WriteConflictException;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.sql.PostgresSqlStructuredTableAdmin;
import io.cdap.cdap.spi.data.sql.SqlStructuredTableRegistry;
import io.cdap.cdap.spi.data.sql.SqlTransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import javax.sql.DataSource;

public class ArtifactAppTest {

  // @ClassRule
  // public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  //
  // private static ArtifactStore artifactStore;
  // private static EmbeddedPostgres pg;
  //
  // @BeforeClass
  // public static void setup() throws Exception {
  //   CConfiguration cConf = CConfiguration.create();
  //   // any plugin which requires transaction will be excluded
  //   cConf.set(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE, Joiner.on(",").join(Table.TYPE, KeyValueTable.TYPE));
  //   Injector injector = AppFabricTestHelper.getInjector(cConf);
  //
  //   pg = EmbeddedPostgres.builder().setDataDirectory(TEMP_FOLDER.newFolder()).setCleanDataDirectory(false).start();
  //   DataSource dataSource = pg.getPostgresDatabase();
  //   SqlStructuredTableRegistry registry = new SqlStructuredTableRegistry(dataSource);
  //   registry.initialize();
  //   StructuredTableAdmin structuredTableAdmin =
  //     new PostgresSqlStructuredTableAdmin(registry, dataSource);
  //   TransactionRunner transactionRunner = new SqlTransactionRunner(structuredTableAdmin, dataSource);
  //   artifactStore = new ArtifactStore(cConf,
  //                                     injector.getInstance(NamespacePathLocator.class),
  //                                     injector.getInstance(LocationFactory.class),
  //                                     injector.getInstance(Impersonator.class),
  //                                     transactionRunner);
  //   StoreDefinition.ArtifactStore.createTables(structuredTableAdmin, false);
  // }
  //
  // @AfterClass
  // public static void afterClass() throws IOException {
  //   pg.close();
  //   AppFabricTestHelper.shutdown();
  // }
  //
  // @After
  // public void cleanup() throws IOException {
  //   artifactStore.clear(NamespaceId.DEFAULT);
  //   artifactStore.clear(NamespaceId.SYSTEM);
  // }
  //
  // @Test
  // public void testGetArtifact() throws Exception {
  //   // add 1 version of another artifact1
  //   Id.Artifact artifact1V1 = Id.Artifact.from(Id.Namespace.DEFAULT, "artifact1", "1.0.0");
  //   String contents1V1 = "first contents v1";
  //   PluginClass plugin1V1 =
  //     new PluginClass("atype", "plugin1", "", "c.c.c.plugin1", "cfg", ImmutableMap.<String, PluginPropertyField>of());
  //   ArtifactMeta meta1V1 = new ArtifactMeta(ArtifactClasses.builder().addPlugin(plugin1V1).build());
  //   writeArtifact(artifact1V1, meta1V1, contents1V1);
  //
  //   // // add 2 versions of an artifact2
  //   // Id.Artifact artifact2V1 = Id.Artifact.from(Id.Namespace.DEFAULT, "artifact2", "0.1.0");
  //   // Id.Artifact artifact2V2 = Id.Artifact.from(Id.Namespace.DEFAULT, "artifact2", "0.1.1");
  //   // Id.Artifact artifact2V3 = Id.Artifact.from(Id.Namespace.DEFAULT, "artifact2", "0.1.1-SNAPSHOT");
  //   // String contents2V1 = "second contents v1";
  //   // String contents2V2 = "second contents v2";
  //   // String contents2V3 = "second contents v3";
  //   // PluginClass plugin2V1 =
  //   //   new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg", ImmutableMap.<String, PluginPropertyField>of());
  //   // PluginClass plugin2V2 =
  //   //   new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg", ImmutableMap.<String, PluginPropertyField>of());
  //   // PluginClass plugin2V3 =
  //   //   new PluginClass("atype", "plugin2", "", "c.c.c.plugin2", "cfg", ImmutableMap.<String, PluginPropertyField>of());
  //   // ArtifactMeta meta2V1 = new ArtifactMeta(ArtifactClasses.builder().addPlugin(plugin2V1).build());
  //   // ArtifactMeta meta2V2 = new ArtifactMeta(ArtifactClasses.builder().addPlugin(plugin2V2).build());
  //   // ArtifactMeta meta2V3 = new ArtifactMeta(ArtifactClasses.builder().addPlugin(plugin2V3).build());
  //   // writeArtifact(artifact2V1, meta2V1, contents2V1);
  //   // writeArtifact(artifact2V2, meta2V2, contents2V2);
  //   // writeArtifact(artifact2V3, meta2V3, contents2V3);
  //
  //   // // test we get 1 version of artifact1 and 2 versions of artifact2
  //   // List<ArtifactDetail> artifact1Versions =
  //   //   artifactStore.getArtifacts(artifact1V1.getNamespace().toEntityId(), artifact1V1.getName(), Integer.MAX_VALUE,
  //   //                              ArtifactSortOrder.UNORDERED);
  //   // Assert.assertEquals(1, artifact1Versions.size());
  //   // assertEqual(artifact1V1, meta1V1, contents1V1, artifact1Versions.get(0));
  // }
  //
  // private void writeArtifact(Id.Artifact artifactId, ArtifactMeta meta, String contents)
  //   throws ArtifactAlreadyExistsException, IOException, WriteConflictException {
  //
  //   File artifactFile = TEMP_FOLDER.newFile();
  //   Files.write(artifactFile.toPath(), Bytes.toBytes(contents));
  //
  //   artifactStore.write(artifactId, meta, artifactFile,
  //                       new EntityImpersonator(artifactId.toEntityId(),
  //                                              new DefaultImpersonator(CConfiguration.create(), null)));
  // }
}