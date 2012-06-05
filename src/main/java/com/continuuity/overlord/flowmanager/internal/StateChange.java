package com.continuuity.overlord.flowmanager.internal;

import com.continuuity.overlord.flowmanager.*;
import com.netflix.curator.framework.CuratorFramework;

/**
 *
 */
public final class StateChange {

  public static class Client {
    public static StateChangeData newState(String accountId, String application, String flowname,
                                         String payload, StateChangeType type) {
      return new StateChangeDataImpl(System.currentTimeMillis(), accountId, application, flowname, payload, type);
    }

    public static StateChanger newState(CuratorFramework client, String path) {
      return new StateChangerImpl(client, path);
    }
  }

  public static class Server {
    public static StateChangeListener newListener(CuratorFramework client) {
      return new StateChangeListenerImpl(client);
    }
  }

}
