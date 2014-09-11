/*
 * Copyright 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.shell;

import co.cask.cdap.shell.completer.PrefixCompleter;
import co.cask.cdap.shell.exception.CommandInputError;
import jline.console.completer.Completer;

import java.io.PrintStream;

/**
 * Command implementation, providing a way to check input arguments and print helper text.
 */
public abstract class AbstractCommand implements Command {

  protected final String argsFormat;
  protected final String description;
  protected String name;

  protected AbstractCommand(String name, String argsFormat, String description) {
    this.name = name;
    this.argsFormat = argsFormat;
    this.description = description;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    verifyArgsFormat(args, argsFormat);
  }

  private void verifyArgsFormat(String[] args, String argsFormat) {
    if (argsFormat != null && args.length < argsFormat.split(" ").length) {
      throw new CommandInputError("Expected arguments: " + argsFormat);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getHelperText(String namePrefix) {
    StringBuilder sb = new StringBuilder();

    if (namePrefix != null && !namePrefix.isEmpty()) {
      sb.append(namePrefix);
      sb.append(' ');
    }

    sb.append(name);

    if (argsFormat != null && !argsFormat.isEmpty()) {
      sb.append(' ');
      sb.append(argsFormat);
    }

    if (description != null && !description.isEmpty()) {
      sb.append(": ");
      sb.append(description);
    }

    return sb.toString();
  }

  protected PrefixCompleter prefixCompleter(String prefix, String additionalPrefix, Completer completer) {
    return new PrefixCompleter(prefix + " " + getName() + additionalPrefix, completer);
  }

  protected PrefixCompleter prefixCompleter(String prefix, Completer completer) {
    return new PrefixCompleter(prefix + " " + getName(), completer);
  }

}
