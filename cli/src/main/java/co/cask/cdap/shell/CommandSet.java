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

import co.cask.cdap.shell.completer.Completable;
import co.cask.cdap.shell.completer.PrefixCompleter;
import co.cask.cdap.shell.completer.StringsCompleter;
import co.cask.cdap.shell.exception.InvalidCommandException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.Completer;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

/**
 * Command representing a set of commands.
 */
public class CommandSet implements Command, Completable {

  private final RadixTree<Command> commandsMap;
  private final List<Command> commands;
  private final String name;

  public CommandSet(String name, List<Command> commands) {
    this.name = name;
    this.commands = ImmutableList.copyOf(commands);
    this.commandsMap = map(this.commands);
  }

  public CommandSet(String name, Command... commands) {
    this.name = name;
    this.commands = ImmutableList.copyOf(commands);
    this.commandsMap = map(this.commands);
  }

  private RadixTree<Command> map(List<Command> commands) {
    RadixTree<Command> result = new ConcurrentRadixTree<Command>(new DefaultCharArrayNodeFactory());
    for (Command command : commands) {
      result.put(command.getName(), command);
    }
    return result;
  }

  @Override
  public void process(String[] args, PrintStream output) throws Exception {
    if (args.length == 0) {
      // TODO: print help message
      throw new InvalidCommandException();
    }

    String commandName = args[0];
    Iterable<Command> matches = commandsMap.getValuesForKeysStartingWith(commandName);
    if (Iterables.isEmpty(matches)) {
      throw new InvalidCommandException();
    }

    Command command = matches.iterator().next();
    command.process(Arrays.copyOfRange(args, 1, args.length), output);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getHelperText(String namePrefix) {
    String realNamePrefix = (namePrefix == null ? "" : namePrefix + " ");
    String realName = (name == null ? "" : name);

    StringBuilder sb = new StringBuilder();
    for (Command command : commands) {
      sb.append(command.getHelperText(realNamePrefix + realName).trim());
      sb.append('\n');
    }

    return sb.toString();
  }

  @Override
  public List<? extends Completer> getCompleters(String prefix) {
    String childPrefix = (prefix == null || prefix.isEmpty() ? "" : prefix + " ") + (name == null ? "" : name);
    List<Completer> completers = Lists.newArrayList();
    List<String> immediateCommands = Lists.newArrayList();

    for (Command command : commands) {
      String name = command.getName();

      if (command instanceof Completable) {
        // add nested completers
        Completable completable = (Completable) command;
        for (Completer completer : completable.getCompleters(childPrefix)) {
          completers.add(completer);
        }
      }

      // add immediate completer
      immediateCommands.add(name);
    }

    if (!childPrefix.isEmpty()) {
      completers.add(new PrefixCompleter(childPrefix, new StringsCompleter(immediateCommands)));
    } else {
      completers.add(new StringsCompleter(immediateCommands));
    }

    return Lists.newArrayList(new AggregateCompleter(completers));
  }

  public static final Builder builder(String name) {
    return new Builder(name);
  }

  /**
   * Builder for {@link CommandSet}.
   */
  public static final class Builder {
    private final String name;
    private final List<Command> commands;

    public Builder(String name) {
      this.name = name;
      this.commands = Lists.newArrayList();
    }

    public Builder addCommand(Command command) {
      commands.add(command);
      return this;
    }

    public CommandSet build() {
      return new CommandSet(name, commands);
    }
  }
}
