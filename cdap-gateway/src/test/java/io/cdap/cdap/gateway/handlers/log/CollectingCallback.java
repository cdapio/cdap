/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers.log;

import co.cask.cdap.logging.read.Callback;
import co.cask.cdap.logging.read.LogEvent;

import java.util.ArrayList;

/**
 * A Callback that simply collects LogEvents into an in-memory array and allows access to them.
 */
public class CollectingCallback implements Callback {
  private ArrayList<LogEvent> logEvents;

  @Override
  public void init() {
    logEvents = new ArrayList<>();
  }

  @Override
  public void handle(LogEvent event) {
    logEvents.add(event);
  }

  @Override
  public int getCount() {
    return logEvents.size();
  }

  @Override
  public void close() {
    // no-op
  }

  public ArrayList<LogEvent> getLogEvents() {
    return logEvents;
  }
}
