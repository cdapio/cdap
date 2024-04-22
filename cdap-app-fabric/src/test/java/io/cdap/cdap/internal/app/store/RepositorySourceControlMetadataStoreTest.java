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

import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.time.Instant;
import org.junit.Test;

public abstract class RepositorySourceControlMetadataStoreTest {

  private static final String NAMESPACE = "testNamespace";
  private static final Instant LAST_MODIFIED = Instant.now();
  private static final String NAME = "testName";
  private static final ApplicationReference APP_REF = new ApplicationReference(NAMESPACE, NAME);
  private static final Boolean IS_SYNCED = true;

  protected static TransactionRunner transactionRunner;

  @Test
  public void testWrite() {
    TransactionRunners.run(transactionRunner, context -> {
      RepositorySourceControlMetadataStore store = RepositorySourceControlMetadataStore.create(
          context);
      store.write(APP_REF, IS_SYNCED, LAST_MODIFIED.toEpochMilli());
      ImmutablePair pair = store.get(APP_REF);
      assertEquals(IS_SYNCED, pair.getSecond());
      assertEquals(LAST_MODIFIED.toEpochMilli(), pair.getFirst());
    });
  }

  @Test
  public void testDelete() {
    TransactionRunners.run(transactionRunner, context -> {
      RepositorySourceControlMetadataStore store = RepositorySourceControlMetadataStore.create(
          context);
      store.write(APP_REF, IS_SYNCED, LAST_MODIFIED.toEpochMilli());
      ImmutablePair pair = store.get(APP_REF);
      assertEquals(pair, new ImmutablePair<>(LAST_MODIFIED.toEpochMilli(), IS_SYNCED));
      store.delete(APP_REF);
      pair = store.get(APP_REF);
      assertEquals(null, pair);
    });
  }
}
