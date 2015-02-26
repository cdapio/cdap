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
 * Exception thrown by the {@link Scheduler} interface in case of unforeseen errors.
 */
public class SchedulerException extends Exception {

  public SchedulerException(String s) {
    super(s);
  }

  public SchedulerException(Throwable throwable) {
    super(throwable);
  }

  public SchedulerException(String s, Throwable throwable) {
    super(s, throwable);
  }
}
