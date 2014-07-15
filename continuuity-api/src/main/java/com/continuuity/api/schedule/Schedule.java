/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.api.schedule;

/**
 * Defines a cron-based schedule for running a program. 
 */
public class Schedule {

  private final String name;

  private final String description;

  private final String cronEntry;

  private final Action action;

  public Schedule(String name, String description, String cronEntry, Action action) {
    this.name = name;
    this.description = description;
    this.cronEntry = cronEntry;
    this.action = action;
  }

  /**
   * @return Name of the schedule.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Schedule description.
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return Cron expression for the schedule.
   */
  public String getCronEntry() {
    return cronEntry;
  }

  /**
   * @return Action for the schedule.
   */
  public Action getAction() {
    return action;
  }

  /**
   * Defines the ScheduleAction.
   */
  public enum Action { START, STOP };
}
