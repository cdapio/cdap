/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.sourcecontrol;

import com.google.gson.Gson;
import com.google.inject.Injector;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.SourceControlManagement;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.internal.app.store.NamespaceSourceControlMetadataStore;
import io.cdap.cdap.proto.artifact.ChangeDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SourceControlMetadataMigrationServiceTest {

  private static TransactionRunner transactionRunner;
  private static CConfiguration cConf;

  private static Store store;

  private static final Gson GSON = new Gson();


  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    AppFabricTestHelper.ensureNamespaceExists(NamespaceId.DEFAULT);
    store = injector.getInstance(Store.class);
    transactionRunner = injector.getInstance(TransactionRunner.class);
    cConf = injector.getInstance(CConfiguration.class);
    cConf.set(SourceControlManagement.METADATA_MIGRATION_INTERVAL_SECONDS, "250");
  }

  @Test
  public void testMigration() {
    Instant now = Instant.now();
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());
    SourceControlMeta metaNew = new SourceControlMeta("hash1", "commit1", now);
    SourceControlMeta metaPresent = new SourceControlMeta("hash", "commit", now);

    // Add 10 applications with scm metadata
    for (int i = 0; i < 10; i++) {
      ApplicationId appId = NamespaceId.DEFAULT.app("appName" + i, "1");
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = context.getTable(
            StoreDefinition.AppMetadataStore.APPLICATION_SPECIFICATIONS);
        ApplicationMeta applicationMeta = new ApplicationMeta(appId.toString(), appSpec,
            new ChangeDetail(null, null, null, now.toEpochMilli()),
            metaNew);

        // Latest with version 2 and metaPresent
        List<Field<?>> fields = getApplicationFields(
            NamespaceId.DEFAULT.getNamespace(),
            appId.getApplication(), "2", applicationMeta,
            new ChangeDetail(null, null, null, now.toEpochMilli()), metaPresent, true);
        table.upsert(fields);

        // Non latest with version 1 and metaNew
        fields = getApplicationFields(
            NamespaceId.DEFAULT.getNamespace(),
            appId.getApplication(), "1", applicationMeta,
            new ChangeDetail(null, null, null, now.toEpochMilli()), metaNew, false);
        table.upsert(fields);
      });
    }

    // Add 5 applications without scm metadata
    for (int i = 10; i < 15; i++) {
      ApplicationId appId = NamespaceId.DEFAULT.app("appName" + i, "1");
      TransactionRunners.run(transactionRunner, context -> {
        StructuredTable table = context.getTable(
            StoreDefinition.AppMetadataStore.APPLICATION_SPECIFICATIONS);
        ApplicationMeta applicationMeta = new ApplicationMeta(appId.toString(), appSpec,
            new ChangeDetail(null, null, null, now.toEpochMilli()),
            metaNew);

        List<Field<?>> fields = getApplicationFields(
            NamespaceId.DEFAULT.getNamespace(),
            appId.getApplication(), "1", applicationMeta,
            new ChangeDetail(null, null, null, now.toEpochMilli()), null, true);
        table.upsert(fields);
      });
    }

    // Add some scm metadata in new table for even id application
    for (int i = 0; i < 10; i += 2) {
      ApplicationReference appRef = NamespaceId.DEFAULT.appReference("appName" + i);
      store.setAppSourceControlMeta(appRef, metaNew);
    }

    // Run one iteration of the migration service.
    SourceControlMetadataMigrationService migrationService =
        new SourceControlMetadataMigrationService(cConf, store, transactionRunner);
    migrationService.runOneIteration();

    // Verify that the scm metadata has been migrated.
    for (int i = 0; i < 10; i++) {
      ApplicationReference appRef = NamespaceId.DEFAULT.appReference("appName" + i);
      // even id applications should have scm metadata not updated
      SourceControlMeta expectedMeta = i % 2 == 0 ? metaNew : metaPresent;
      expectedMeta = SourceControlMeta.builder(expectedMeta).setSyncStatus(true).build();
      Assert.assertEquals(expectedMeta, store.getAppSourceControlMeta(appRef));
    }

    // Verify that the scm metadata has been migrated for applications without scm metadata.
    for (int i = 10; i < 15; i++) {
      ApplicationReference appId = NamespaceId.DEFAULT.appReference("appName" + i);
      TransactionRunners.run(transactionRunner, context -> {
        NamespaceSourceControlMetadataStore store = NamespaceSourceControlMetadataStore.create(
            context);
        SourceControlMeta gotMeta = store.get(appId);
        org.junit.Assert.assertEquals(SourceControlMeta.createDefaultMeta(), gotMeta);
      });
    }
  }


  // Needed as the current write application don't store scm metadata anymore
  private List<Field<?>> getApplicationFields(String namespaceId, String appId, String versionId,
      ApplicationMeta appMeta, ChangeDetail change, SourceControlMeta scmMeta,
      boolean markAsLatest) {
    List<Field<?>> fields = new ArrayList<>();
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.NAMESPACE_FIELD, namespaceId));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.APPLICATION_FIELD, appId));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.VERSION_FIELD, versionId));
    fields.add(
        Fields.stringField(StoreDefinition.AppMetadataStore.APPLICATION_DATA_FIELD,
            GSON.toJson(appMeta)));
    if (change != null) {
      fields.add(
          Fields.stringField(StoreDefinition.AppMetadataStore.AUTHOR_FIELD, change.getAuthor()));
      fields.add(Fields.longField(StoreDefinition.AppMetadataStore.CREATION_TIME_FIELD,
          change.getCreationTimeMillis()));
      fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.CHANGE_SUMMARY_FIELD,
          change.getDescription()));
    }
    fields.add(Fields.booleanField(StoreDefinition.AppMetadataStore.LATEST_FIELD, markAsLatest));
    fields.add(Fields.stringField(StoreDefinition.AppMetadataStore.SOURCE_CONTROL_META,
        GSON.toJson(scmMeta)));
    return fields;
  }
}
