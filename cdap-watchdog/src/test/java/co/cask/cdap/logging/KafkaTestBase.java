/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.logging;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.kerberos.DefaultOwnerAdmin;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.runtime.TransactionExecutorModule;
import co.cask.cdap.kafka.KafkaTester;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import org.apache.tephra.runtime.TransactionModules;
import org.junit.ClassRule;

/**
 * Base test class that start up Kafka during at the beginning of the test, and stop Kafka when test is done.
 */
public abstract class KafkaTestBase {
  @ClassRule
  public static final KafkaTester KAFKA_TESTER = new KafkaTester(
    ImmutableMap.<String, String>builder()
      .put(Constants.Logging.NUM_PARTITIONS, "2")
      .put(LoggingConfiguration.KAFKA_PRODUCER_TYPE, "sync")
      .put(LoggingConfiguration.KAFKA_PRODUCER_BUFFER_MS, "100")
      .put(LoggingConfiguration.LOG_RETENTION_DURATION_DAYS, "10")
      .put(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, "10240")
      .put("log.pipeline.cdap.file.sync.interval.bytes", "5120")
      .build(),
    ImmutableList.of(
      new NonCustomLocationUnitTestModule().getModule(),
      new TransactionModules().getInMemoryModules(),
      new TransactionExecutorModule(),
      new DataSetsModules().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getNoOpModule(),
      new LoggingModules().getDistributedModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
          bind(NamespaceQueryAdmin.class).to(SimpleNamespaceQueryAdmin.class);
        }
      }
    ),
    2,
    LoggingConfiguration.KAFKA_SEED_BROKERS
  );
}
