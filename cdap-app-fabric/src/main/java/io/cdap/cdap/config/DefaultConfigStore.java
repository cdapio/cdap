/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.config;

import com.google.inject.Inject;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;

import java.util.List;

/**
 * Default Configuration Store. A "configuration" consists of a namespace, type, name, and properties map
 * The primary key is namespace, type, and name.
 *
 * For example, the ConsoleSettingsStore uses this to store user-specific configurations.
 * The type is "usersettings", and name is the user name.
 * "
 */
public class DefaultConfigStore implements ConfigStore {
  private final TransactionRunner transactionRunner;

  @Inject
  public DefaultConfigStore(TransactionRunner transactionRunner) {
    this.transactionRunner = transactionRunner;
  }

  @Override
  public void create(String namespace, String type, Config config) throws ConfigExistsException {
    TransactionRunners.run(transactionRunner, context -> {
      ConfigTable table = new ConfigTable(context);
      table.create(namespace, type, config);
    }, ConfigExistsException.class);
  }

  @Override
  public void createOrUpdate(String namespace, String type, Config config) {
    TransactionRunners.run(transactionRunner, context -> {
      ConfigTable table = new ConfigTable(context);
      table.createOrUpdate(namespace, type, config);
    });
  }

  @Override
  public void delete(String namespace, String type, String name) throws ConfigNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      ConfigTable table = new ConfigTable(context);
      table.delete(namespace, type, name);
    }, ConfigNotFoundException.class);
  }

  @Override
  public List<Config> list(String namespace, String type) {
    return TransactionRunners.run(transactionRunner, context -> {
      ConfigTable table = new ConfigTable(context);
      return table.list(namespace, type);
    });
  }

  @Override
  public Config get(String namespace, String type, String name) throws ConfigNotFoundException {
    return TransactionRunners.run(transactionRunner, context -> {
      ConfigTable table = new ConfigTable(context);
      return table.get(namespace, type, name);
    }, ConfigNotFoundException.class);
  }

  @Override
  public void update(String namespace, String type, Config config) throws ConfigNotFoundException {
    TransactionRunners.run(transactionRunner, context -> {
      ConfigTable table = new ConfigTable(context);
      table.update(namespace, type, config);
    }, ConfigNotFoundException.class);
  }
}
