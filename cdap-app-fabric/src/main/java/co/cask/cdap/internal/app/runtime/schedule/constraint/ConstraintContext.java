/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.constraint;

import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.RunRecord;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Context object, exposing information that may be useful during checking constraints.
 */
public final class ConstraintContext {
  private final long checkTime;
  private final Notification notification;

  public ConstraintContext(long checkTime, Notification notification) {
    this.checkTime = checkTime;
    this.notification = notification;
  }

  public long getCheckTime() {
    return checkTime;
  }

  public List<RunRecord> getProgramRuns(@Nullable ProgramRunStatus status) {
    // TODO: implement
    throw new UnsupportedOperationException();
  }

  public List<RunRecord> getProgramRuns(@Nullable ProgramRunStatus status, long startTime, long endTime, int limit) {
    // TODO: implement
    throw new UnsupportedOperationException();
  }

  public Notification getNotification() {
    return notification;
  }
}
