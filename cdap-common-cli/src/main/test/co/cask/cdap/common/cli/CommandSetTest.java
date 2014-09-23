/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.common.cli;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Test for {@link CommandSet}.
 */
public class CommandSetTest {

  @Test
  public void testFindMatch() throws Exception {
    Command greetCommand = new Command() {
      @Override
      public void execute(Arguments arguments, PrintStream output) throws Exception {
        output.println("truncated!");
      }

      @Override
      public String getPattern() {
        return "truncate all streams";
      }

      @Override
      public String getDescription() {
        return "Truncates all streams";
      }
    };

    CommandSet commandSet = new CommandSet<Command>(ImmutableList.of(greetCommand));
    CommandMatch match = commandSet.findMatch("truncate all streams");
    Assert.assertTrue(match.getCommand() == greetCommand);
    testCommand(match.getCommand(), match.getArguments(), "truncated!\n");

    Assert.assertNull(commandSet.findMatch("truncate all streams!"));
    Assert.assertNull(commandSet.findMatch("truncate no streams"));
    Assert.assertNull(commandSet.findMatch("truncate all streams x"));
    Assert.assertNull(commandSet.findMatch("x truncate all streams"));
  }

  @Test
  public void testFindMatchWithArguments() throws Exception {
    Command greetCommand = new Command() {
      @Override
      public void execute(Arguments arguments, PrintStream output) throws Exception {
        for (int i = 0; i < arguments.getInt("times", 1); i++) {
          output.println("Hello " + arguments.get("user"));
        }
      }

      @Override
      public String getPattern() {
        return "greet <user> <times>";
      }

      @Override
      public String getDescription() {
        return "Greets a user";
      }
    };

    CommandSet commandSet = new CommandSet<Command>(ImmutableList.of(greetCommand));
    CommandMatch match = commandSet.findMatch("greet bob 5");
    Assert.assertTrue(match.getCommand() == greetCommand);
    testCommand(match.getCommand(), match.getArguments(), Strings.repeat("Hello bob\n", 5));
  }

  @Test
  public void testFindMatchWithOptionalArguments() throws Exception {
    Command greetCommand = new Command() {
      @Override
      public void execute(Arguments arguments, PrintStream output) throws Exception {
        for (int i = 0; i < arguments.getInt("times", 1); i++) {
          output.printf("[%d] Hello %s %s\n", arguments.getInt("timestamp", 111),
                        arguments.get("user"), arguments.get("suffix", "oneoneone"));
        }
      }

      @Override
      public String getPattern() {
        return "greet <user> <times> [timestamp] [suffix]";
      }

      @Override
      public String getDescription() {
        return "Greets a user";
      }
    };

    CommandSet commandSet = new CommandSet<Command>(ImmutableList.of(greetCommand));
    CommandMatch match = commandSet.findMatch("greet bob 5 123 blah");
    Assert.assertTrue(match.getCommand() == greetCommand);
    testCommand(match.getCommand(), match.getArguments(), Strings.repeat("[123] Hello bob blah\n", 5));

    match = commandSet.findMatch("greet bob 5");
    Assert.assertTrue(match.getCommand() == greetCommand);
    testCommand(match.getCommand(), match.getArguments(), Strings.repeat("[111] Hello bob oneoneone\n", 5));
  }

  private void testCommand(Command command, Arguments args, String expectedOutput) throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);
    command.execute(args, printStream);

    String output = new String(outputStream.toByteArray(), Charsets.UTF_8);
    Assert.assertEquals(expectedOutput, output);
  }
}
