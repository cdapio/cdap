package com.continuuity.overlord.flowmanager.internal;

import com.continuuity.overlord.flowmanager.StateChangeException;
import com.continuuity.overlord.flowmanager.StateChanger;
import com.continuuity.overlord.flowmanager.StateChangeData;
import com.google.gson.Gson;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.queue.SimpleDistributedQueue;

/**
 *
 */
final class StateChangerImpl implements StateChanger {
  private final CuratorFramework client;
  private final Gson gson = new Gson();
  private final SimpleDistributedQueue queue;

  public StateChangerImpl(CuratorFramework client, String path) {
    this.client = client;
    this.queue = new SimpleDistributedQueue(client, path);
  }

  @Override
  public void change(StateChangeData data) throws StateChangeException {
    byte[] bytes = gson.toJson(data).getBytes();
    try {
      this.queue.offer(bytes);
    } catch (Exception e) {
      throw new StateChangeException("Unable change state. Data : " + data.toString());
    }
  }
}
