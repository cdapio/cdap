/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.app.deploy;

import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Information required by {@link io.cdap.cdap.app.deploy.ProgramRunDispatcher} to execute
 * program-run logic.
 */
public class ProgramRunDispatcherContext {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramRunDispatcherContext.class);

  private final ProgramDescriptor programDescriptor;
  private final RunId runId;
  private final ProgramOptions programOptions;
  private final boolean distributed;

  // Transient to avoid serialization of tasks
  private final transient Deque<Runnable> cleanupTasks;

  public ProgramRunDispatcherContext(ProgramRunDispatcherContext other) {
    this(other.programDescriptor, other.programOptions, other.runId, other.distributed);
  }

  public ProgramRunDispatcherContext(ProgramDescriptor programDescriptor,
      ProgramOptions programOptions,
      RunId runId, boolean distributed) {
    this.programDescriptor = programDescriptor;
    this.programOptions = programOptions;
    this.runId = runId;
    this.distributed = distributed;
    this.cleanupTasks = new ConcurrentLinkedDeque<>();
  }

  public ProgramOptions getProgramOptions() {
    return programOptions;
  }

  public ProgramDescriptor getProgramDescriptor() {
    return programDescriptor;
  }

  public RunId getRunId() {
    return runId;
  }

  public boolean isDistributed() {
    return distributed;
  }

  public void addCleanupTask(Runnable task) {
    cleanupTasks.add(task);
  }

  public void executeCleanupTasks() {
    Iterator<Runnable> iter = cleanupTasks.descendingIterator();
    while (iter.hasNext()) {
      try {
        iter.next().run();
      } catch (Exception e) {
        LOG.warn("Exception raised when executing cleanup task", e);
      }
      iter.remove();
    }
  }
}
