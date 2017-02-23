/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tephra.txprune;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TxConstants;
import org.apache.twill.internal.utils.Instances;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Service to prune the invalid list periodically.
 */
public class TransactionPruningService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionPruningService.class);

  private final Configuration conf;
  private final TransactionManager txManager;
  private final long scheduleInterval;
  private final boolean pruneEnabled;
  private ScheduledExecutorService scheduledExecutorService;

  public TransactionPruningService(Configuration conf, TransactionManager txManager) {
    this.conf = conf;
    this.txManager = txManager;
    this.pruneEnabled = conf.getBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE,
                                        TxConstants.TransactionPruning.DEFAULT_PRUNE_ENABLE);
    this.scheduleInterval = conf.getLong(TxConstants.TransactionPruning.PRUNE_INTERVAL,
                                         TxConstants.TransactionPruning.DEFAULT_PRUNE_INTERVAL);
  }

  @Override
  protected void startUp() throws Exception {
    if (!pruneEnabled) {
      LOG.info("Transaction pruning is not enabled");
      return;
    }

    LOG.info("Starting {}...", this.getClass().getSimpleName());
    scheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                                                   .setNameFormat("tephra-pruning-thread")
                                                   .setDaemon(true)
                                                   .build());

    Map<String, TransactionPruningPlugin> plugins = initializePlugins();
    long txMaxLifetimeMillis = TimeUnit.SECONDS.toMillis(conf.getInt(TxConstants.Manager.CFG_TX_MAX_LIFETIME,
                                                                     TxConstants.Manager.DEFAULT_TX_MAX_LIFETIME));
    long txPruneBufferMillis =
      TimeUnit.SECONDS.toMillis(conf.getLong(TxConstants.TransactionPruning.PRUNE_GRACE_PERIOD,
                                            TxConstants.TransactionPruning.DEFAULT_PRUNE_GRACE_PERIOD));
    scheduledExecutorService.scheduleAtFixedRate(
      getTxPruneRunnable(txManager, plugins, txMaxLifetimeMillis, txPruneBufferMillis),
      scheduleInterval, scheduleInterval, TimeUnit.SECONDS);
    LOG.info("Scheduled {} plugins with interval {} seconds", plugins.size(), scheduleInterval);
  }

  @Override
  protected void shutDown() throws Exception {
    if (!pruneEnabled) {
      return;
    }

    LOG.info("Stopping {}...", this.getClass().getSimpleName());
    try {
      scheduledExecutorService.shutdown();
      scheduledExecutorService.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      scheduledExecutorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
    LOG.info("Stopped {}", this.getClass().getSimpleName());
  }

  @VisibleForTesting
  TransactionPruningRunnable getTxPruneRunnable(TransactionManager txManager,
                                                Map<String, TransactionPruningPlugin> plugins,
                                                long txMaxLifetimeMillis, long txPruneBufferMillis) {
    return new TransactionPruningRunnable(txManager, plugins, txMaxLifetimeMillis, txPruneBufferMillis);
  }

  private Map<String, TransactionPruningPlugin> initializePlugins()
    throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
    InstantiationException, IOException {
    Map<String, TransactionPruningPlugin> initializedPlugins = new HashMap<>();

    // Read set of plugin names from configuration
    Set<String> plugins =
      new HashSet<>(Arrays.asList(conf.getTrimmedStrings(TxConstants.TransactionPruning.PLUGINS,
                                                         TxConstants.TransactionPruning.DEFAULT_PLUGIN)));

    LOG.info("Initializing invalid list prune plugins {}", plugins);
    for (String plugin : plugins) {
      // Load the class for the plugin
      // TODO: TEPHRA-205 classloader isolation for plugins
      Class<? extends TransactionPruningPlugin> clazz = null;
      if (TxConstants.TransactionPruning.DEFAULT_PLUGIN.equals(plugin)) {
        Class<?> defaultClass = Class.forName(TxConstants.TransactionPruning.DEFAULT_PLUGIN_CLASS);
        if (TransactionPruningPlugin.class.isAssignableFrom(defaultClass)) {
          //noinspection unchecked
          clazz = (Class<? extends TransactionPruningPlugin>) defaultClass;
        }
      } else {
        clazz = conf.getClass(plugin + TxConstants.TransactionPruning.PLUGIN_CLASS_SUFFIX,
                              null, TransactionPruningPlugin.class);
      }
      if (clazz == null) {
        throw new IllegalStateException("No class specified in configuration for invalid pruning plugin " + plugin);
      }
      LOG.debug("Got class {} for plugin {}", clazz.getName(), plugin);

      TransactionPruningPlugin instance = Instances.newInstance(clazz);
      instance.initialize(conf);
      LOG.debug("Plugin {} initialized", plugin);
      initializedPlugins.put(plugin, instance);
    }

    return initializedPlugins;
  }
}
