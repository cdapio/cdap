/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.cli;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.commons.cli.Options;

import java.util.Arrays;
import java.util.List;

/**
 * Represents the command line arguments for {@link CLIMain}.
 */
public class CLIMainArgs {

  private final String[] optionTokens;
  private final String[] commandTokens;

  public CLIMainArgs(String[] optionTokens, String[] commandTokens) {
    this.optionTokens = optionTokens;
    this.commandTokens = commandTokens;
  }

  public String[] getOptionTokens() {
    return optionTokens;
  }

  public String[] getCommandTokens() {
    return commandTokens;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(Arrays.hashCode(optionTokens), Arrays.hashCode(commandTokens));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final CLIMainArgs other = (CLIMainArgs) obj;
    return Arrays.equals(this.optionTokens, other.optionTokens) &&
      Arrays.equals(this.commandTokens, other.commandTokens);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("optionTokens", Arrays.toString(optionTokens))
      .add("commandTokens", Arrays.toString(commandTokens)).toString();
  }

  public static CLIMainArgs parse(String[] args, Options options) {
    List<String> optionsPart = Lists.newArrayList();
    List<String> commandPart = Lists.newArrayList();

    boolean inOptionsPart = true;
    for (int i = 0; i < args.length; i++) {
      String token = args[i];
      if (inOptionsPart) {
        if (!token.startsWith("-")) {
          inOptionsPart = false;
        } else {
          if (!options.getOption(token).hasArg()) {
            inOptionsPart = true;
          } else if (options.getOption(token).hasArg() && i + 1 < args.length) {
            inOptionsPart = true;
            // add the option and option value
            optionsPart.add(token);
            optionsPart.add(args[++i]);
            continue;
          } else {
            inOptionsPart = false;
          }
        }
      }

      if (inOptionsPart) {
        optionsPart.add(token);
      } else {
        commandPart.add(token);
      }
    }
    return new CLIMainArgs(optionsPart.toArray(new String[optionsPart.size()]),
                           commandPart.toArray(new String[commandPart.size()]));
  }
}
