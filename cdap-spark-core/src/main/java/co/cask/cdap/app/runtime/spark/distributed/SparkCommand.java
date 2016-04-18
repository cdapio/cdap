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

package co.cask.cdap.app.runtime.spark.distributed;

import org.apache.twill.api.Command;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Command used by {@link SparkExecutionService}.
 */
public class SparkCommand implements Command {

  /**
   * Command for stopping the spark execution.
   */
  public static final SparkCommand STOP = new SparkCommand("stop");

  private final String command;
  private final Map<String, String> options;

  public SparkCommand(String command) {
    this(command, Collections.<String, String>emptyMap());
  }

  public SparkCommand(String command, Map<String, String> options) {
    this.command = command;
    this.options = Collections.unmodifiableMap(new HashMap<>(options));
  }

  @Override
  public String getCommand() {
    return command;
  }

  @Override
  public Map<String, String> getOptions() {
    return options;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkCommand that = (SparkCommand) o;
    return Objects.equals(command, that.command) && Objects.equals(options, that.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(command, options);
  }
}
