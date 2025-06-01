/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.master.export_import;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.store.NamespaceTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.cdap.cdap.proto.NamespaceMeta;

public class ExportJobMain
{
  private final static Logger LOG = LoggerFactory.getLogger(io.cdap.cdap.ExportJobMain.class);
  public void exportNamespaces() {
    CConfiguration cConf = CConfiguration.create();
    List<Module> modules = new ArrayList<>(Arrays.asList(
        new ConfigModule(cConf),
        new StorageModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class)
                .in(Scopes.SINGLETON);
          }
        }
    ));
    Injector injector = Guice.createInjector(modules);
    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
    LOG.debug("Starting export of namespaces");
    TransactionRunners.run(transactionRunner, context -> {
      NamespaceTable namespaceTable = new NamespaceTable(context);
      List<NamespaceMeta> namespaces = namespaceTable.list();
      LOG.debug("Found {} namespaces: {}", namespaces.size(), namespaces);
    });
    LOG.debug("Finished exporting namespaces.");
  }
  public static void main( String[] args )
  {
    LOG.debug("Args: {}", args);
    io.cdap.cdap.ExportJobMain exportJob = new io.cdap.cdap.ExportJobMain();
    exportJob.exportNamespaces();
    System.out.println("Job finished.");
  }
}
