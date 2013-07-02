/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.zookeeper.NodeChildren;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.OperationFuture;
import com.continuuity.weave.zookeeper.ZKClient;
import com.continuuity.weave.zookeeper.ZKOperations;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link BrokerService} that watches kafka zk nodes for updates of broker lists and leader for
 * each topic partition.
 */
public final class ZKBrokerService extends AbstractIdleService implements BrokerService {

  private static final Logger LOG = LoggerFactory.getLogger(ZKBrokerService.class);
  private static final String BROKER_IDS_PATH = "/brokers/ids";
  private static final Gson GSON = new Gson();

  private final ZKClient zkClient;
  private final AtomicReference<Map<Integer, BrokerInfo>> brokerInfos;

  private Cancellable cancelWatchers;

  public ZKBrokerService(ZKClient zkClient) {
    this.zkClient = zkClient;
    this.brokerInfos = new AtomicReference<Map<Integer, BrokerInfo>>(ImmutableMap.<Integer, BrokerInfo>of());
  }

  @Override
  protected void startUp() throws Exception {
    // Watch for changes in brokers
    final Cancellable cancelBrokerWatch = watchBrokerInfos();

    cancelWatchers = new Cancellable() {
      @Override
      public void cancel() {
        cancelBrokerWatch.cancel();
      }
    };
  }

  @Override
  protected void shutDown() throws Exception {
    cancelWatchers.cancel();
  }

  @Override
  public BrokerInfo getLeader(String topic, int partition) {
    return null;
  }

  /**
   * Watch for updates of broker information.
   * @return A {@link Cancellable} for cancelling the watch.
   */
  private Cancellable watchBrokerInfos() {
    return ZKOperations.watchChildren(zkClient, BROKER_IDS_PATH, new ZKOperations.ChildrenCallback() {
      @Override
      public void updated(NodeChildren nodeChildren) {
        // For each id node, fetch the data to get hold of broker connection info.
        ImmutableList.Builder<OperationFuture<NodeData>> builder = ImmutableList.builder();
        for (String id : nodeChildren.getChildren()) {
          builder.add(zkClient.getData(BROKER_IDS_PATH + "/" + id));
        }
        // Wait for all data fetch being completed.
        final List<OperationFuture<NodeData>> futures = builder.build();
        Futures.successfulAsList(futures).addListener(new Runnable() {
          @Override
          public void run() {
            // Decode each broker info and update the map.
            ImmutableMap.Builder<Integer, BrokerInfo> infos = ImmutableMap.builder();
            for (OperationFuture<NodeData> future : futures) {
              try {
                BrokerInfo brokerInfo = decodeBrokerInfo(future.get());
                if (brokerInfo != null) {
                  infos.put(getBrokerIdFromPath(future.getRequestPath()), brokerInfo);
                }
              } catch (Exception e) {
                // Ignore fetch error.
                LOG.info("Error to fetch data for {}", future.getRequestPath(), e);
              }
            }
            brokerInfos.set(infos.build());
          }
        }, Threads.SAME_THREAD_EXECUTOR);
      }
    });
  }

  private BrokerInfo decodeBrokerInfo(NodeData nodeData) {
    byte[] data = nodeData == null ? null : nodeData.getData();
    if (data == null) {
      return null;
    }
    return GSON.fromJson(new String(data, Charsets.UTF_8), BrokerInfo.class);
  }

  private Cancellable watchPartitionInfo() {
    return null;
  }

  private int getBrokerIdFromPath(String path) {
    return Integer.parseInt(path.substring(BROKER_IDS_PATH.length() + 1));
  }
}
