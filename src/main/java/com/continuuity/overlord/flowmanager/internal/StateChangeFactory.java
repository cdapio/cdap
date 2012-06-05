package com.continuuity.overlord.flowmanager.internal;

import com.continuuity.overlord.flowmanager.*;
import com.netflix.curator.framework.CuratorFramework;

/**
 *
 */
public final class StateChangeFactory {

  public static StateChangeData newState(long timestamp, String accountId, String application, String flowname,
                                         String payload, StateChangeType type) {
    return new StateChangeDataImpl(timestamp, accountId, application, flowname, payload, type);
  }

  public static StateChangeListener newListener(CuratorFramework client) {
    return new StateChangeListenerImpl(client);
  }

  public static StateChanger newChange(CuratorFramework client, String path) {
    return new StateChangerImpl(client, path);
  }
}
