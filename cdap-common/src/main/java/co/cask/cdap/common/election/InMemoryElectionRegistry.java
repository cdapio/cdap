/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.election;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Singleton;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * In memory version of leader election.
 */
@Singleton
public final class InMemoryElectionRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryElectionRegistry.class);

  private final Multimap<String, RegistryElectionHandler> registry;

  // A single thread executor to perform all leader election related operations (register, cancel, callback).
  private ExecutorService executor;

  public InMemoryElectionRegistry() {
    // It has to be a linked hash multimap as the order of addition determine who is the leader.
    this.registry = LinkedHashMultimap.create();
    // Creates a single thread executor using daemon threads. No need to worry about shutdown.
    this.executor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("memory-leader-election"));
  }

  /**
   * Joins the leader election process of the given named group.
   *
   * @param name The group name for performing leader election
   * @param handler The callback for acting on leader election events
   * @return A {@link Cancellable} to withdraw from the process
   */
  public Cancellable join(final String name, ElectionHandler handler) {
    final RegistryElectionHandler registryHandler = new RegistryElectionHandler(name, handler);

    executor.execute(new Runnable() {
      @Override
      public void run() {
        registry.put(name, registryHandler);

        // Fetch the first handler from the registry map. If it is the same as the newly added one, notify it as leader.
        // The iterator shouldn't be empty
        if (registryHandler == registry.get(name).iterator().next()) {
          registryHandler.leader();
        } else {
          registryHandler.follower();
        }
      }
    });

    // Return a Cancellable that remove the handler from the registry map and notify it become follower.
    // It also select the new leader and notify it.
    return new Cancellable() {
      @Override
      public void cancel() {
        Futures.getUnchecked(executor.submit(new Runnable() {
          @Override
          public void run() {
            // If not in register or not a leader, simply return.
            if (!registry.remove(name, registryHandler) || !registryHandler.isLeader()) {
              return;
            }

            // The removed handler is the leader. Notify that it becomes follower
            registryHandler.follower();

            // Try to find a new leader. It'll be the first in the list.
            Iterator<RegistryElectionHandler> iterator = registry.get(name).iterator();
            if (!iterator.hasNext()) {
              return;
            }
            // Schedule notification of the new leader.
            final RegistryElectionHandler leader = iterator.next();
            executor.submit(new Runnable() {
              @Override
              public void run() {
                Iterator<RegistryElectionHandler> iterator = registry.get(name).iterator();
                // Make sure the leader hasn't changed.
                // If the leader changed, the adding/removal logic should already handler notification.
                if (iterator.hasNext() && iterator.next() == leader) {
                  leader.leader();
                }
              }
            });
          }
        }));
      }
    };
  }

  /**
   * Wrapper for the ElectionHandler callback that helps to maintain state of the handler.
   */
  private final class RegistryElectionHandler implements ElectionHandler {

    private final String name;
    private final ElectionHandler delegate;
    private final UUID id;
    private boolean leader;

    private RegistryElectionHandler(String name, ElectionHandler delegate) {
      this.name = name;
      this.delegate = delegate;
      this.id = UUID.randomUUID();
    }

    @Override
    public void leader() {
      leader = true;
      try {
        delegate.leader();
      } catch (Throwable t) {
        LOG.warn("Exception thrown from ElectionHandler.leader(). Withdraw from leader election process.", t);
        registry.remove(name, this);
        leader = false;
      }
    }

    @Override
    public void follower() {
      leader = false;
      try {
        delegate.follower();
      } catch (Throwable t) {
        LOG.warn("Exception thrown from ElectionHandler.follower(). Withdraw from leader election process.", t);
        registry.remove(name, this);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      RegistryElectionHandler other = (RegistryElectionHandler) o;
      return id.equals(other.id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }

    public boolean isLeader() {
      return leader;
    }
  }
}
