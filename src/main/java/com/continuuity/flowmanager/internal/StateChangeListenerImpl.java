package com.continuuity.flowmanager.internal;

import com.continuuity.flowmanager.StateChangeCallback;
import com.continuuity.flowmanager.StateChangeData;
import com.continuuity.flowmanager.StateChangeListenerException;
import com.continuuity.flowmanager.StateChangeListener;
import com.google.gson.Gson;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.queue.SimpleDistributedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.TimeUnit;

/**
 *
 */
final class StateChangeListenerImpl implements StateChangeListener {
  private static final Logger Log = LoggerFactory.getLogger(StateChangeListenerImpl.class);
  private final CuratorFramework client;
  private SimpleDistributedQueue queue;
  private volatile boolean running;
  private final Gson gson = new Gson();

  public StateChangeListenerImpl(CuratorFramework client) {
    this.client = client;
    this.running = false;
  }

  @Override
  public void listen(final String path, final StateChangeCallback callback) throws StateChangeListenerException {
    queue = new SimpleDistributedQueue(client, path);
    running = true;
    new Thread(new Runnable() {
      @Override
      public void run() {
        while(running) {
          try {
            byte[] bytes = queue.poll(5, TimeUnit.SECONDS);
            StateChangeData data  = gson.fromJson(new StringReader(new String(bytes)), StateChangeDataImpl.class);
            if(data == null) {
              continue;
            }
            callback.process(data);
          } catch (Exception e) {
            /** Generally the event rate is low, so it's possible that we might get this */
          }
        }
      }
    }).start();
  }


  @Override
  public void close() throws IOException {
    this.running = false;
  }
}
