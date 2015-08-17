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

package co.cask.cdap.data.tools;

/**
 * {@link HBaseQueueDebugger} with simple defaults.
 */
public class SimpleHBaseQueueDebugger {
  public static void main(String[] args) throws Exception {
    if (args.length >= 1 && args[0].equals("help")) {
      System.out.printf("Displays the minimum transaction time for all events in all queues.\n\n");
      System.out.printf("Uses HBaseQueueDebugger with the following options:\n");
      System.out.printf("-D" + HBaseQueueDebugger.PROP_SHOW_PROGRESS + "=false\n");
      System.out.printf("-D" + HBaseQueueDebugger.PROP_SHOW_TX_TIMESTAMP_ONLY + "=true\n");
      return;
    }

    System.setProperty(HBaseQueueDebugger.PROP_SHOW_PROGRESS, "false");
    System.setProperty(HBaseQueueDebugger.PROP_SHOW_TX_TIMESTAMP_ONLY, "true");
    HBaseQueueDebugger.main(args);
  }
}
