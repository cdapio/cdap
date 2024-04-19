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

package io.cdap.cdap.internal.app.store;

import static org.junit.Assert.assertEquals;

import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.time.Instant;
import org.junit.Before;
import org.junit.Test;

public abstract class SourceControlMetadataStoreTest {

  private static final String NAMESPACE = "testNamespace";
  private static final String COMMIT_ID = "testCommitId";
  private static final String SPEC_HASH = "testSpecHash";
  private static final Instant LAST_MODIFIED = Instant.now();
  private static final String NAME = "testName";
  private static final ApplicationId APP_ID = new ApplicationId(NAMESPACE, NAME);
  private static final SourceControlMeta SOURCE_CONTROL_META = new SourceControlMeta(SPEC_HASH,
      COMMIT_ID, LAST_MODIFIED);

  protected static TransactionRunner transactionRunner;

  @Before
  public void before() {
    TransactionRunners.run(transactionRunner, context -> {
      SourceControlMetadataStore store = SourceControlMetadataStore.create(context);
      store.deleteNamespaceSourceControlMetadataTable();
    });

    TransactionRunners.run(transactionRunner, context -> {
      SourceControlMetadataStore store = SourceControlMetadataStore.create(context);
      store.write(APP_ID, SOURCE_CONTROL_META);
    });
  }

  @Test
  public void testSetNamespaceSourceControlMeta() throws Exception {
    TransactionRunners.run(transactionRunner, context -> {
      SourceControlMetadataStore store = SourceControlMetadataStore.create(context);
      ApplicationId appId = new ApplicationId(NAMESPACE, "test2");
      store.write(appId, SOURCE_CONTROL_META);
      SourceControlMeta sourceControlMeta = store.get(appId);
      assertEquals(SOURCE_CONTROL_META, sourceControlMeta);
    });
  }

  @Test
  public void testGetNamespaceSourceControlMeta() throws Exception {
    TransactionRunners.run(transactionRunner, context -> {
      SourceControlMetadataStore store = SourceControlMetadataStore.create(context);
      SourceControlMeta scmMeta = store.get(APP_ID);
      assertEquals(SOURCE_CONTROL_META, scmMeta);
    });
  }

  @Test
  public void testUpdateNamespaceSourceControlMeta() throws Exception {
    SourceControlMeta newSourceControlMeta = new SourceControlMeta("newFileHash", "newCommitId",
        Instant.now());
    TransactionRunners.run(transactionRunner, context -> {
      SourceControlMetadataStore store = SourceControlMetadataStore.create(context);
      store.write(APP_ID, newSourceControlMeta);
      SourceControlMeta scmMeta = store.get(APP_ID);
      assertEquals(newSourceControlMeta, scmMeta);
    });
  }

  @Test
  public void testDeleteNamespaceSourceControlMetadata() throws Exception {
    TransactionRunners.run(transactionRunner, context -> {
      SourceControlMetadataStore store = SourceControlMetadataStore.create(context);
      store.delete(APP_ID);
      SourceControlMeta scmMeta = store.get(APP_ID);
      assertEquals(null, scmMeta);
    });
  }
}
