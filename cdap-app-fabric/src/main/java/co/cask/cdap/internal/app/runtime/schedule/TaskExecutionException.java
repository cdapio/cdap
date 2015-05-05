/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule;

/**
 * Exception fired when the execution of a scheduled task has failed.
 */
public class TaskExecutionException extends Exception {

  private final boolean refireImmediately;

  public TaskExecutionException(String msg, Throwable t, boolean refireImmediately) {
    super(msg, t);
    this.refireImmediately = refireImmediately;
  }

  public TaskExecutionException(Throwable t, boolean refireImmediately) {
    super(t);
    this.refireImmediately = refireImmediately;
  }

  public TaskExecutionException(String msg, boolean refireImmediately) {
    super(msg);
    this.refireImmediately = refireImmediately;
  }

  public boolean isRefireImmediately() {
    return refireImmediately;
  }
}
