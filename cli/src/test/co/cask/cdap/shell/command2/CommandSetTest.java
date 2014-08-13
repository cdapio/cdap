/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.shell.command2;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
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

    CommandSet commandSet = new CommandSet(greetCommand);
    CommandMatch match = commandSet.findMatch("greet bob 5");
    Assert.assertTrue(match.getCommand() == greetCommand);


    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    PrintStream printStream = new PrintStream(outputStream);

    Command matchedCommand = match.getCommand();
    matchedCommand.execute(match.getArguments(), printStream);

    String output = new String(outputStream.toByteArray(), Charsets.UTF_8);
    Assert.assertEquals(Strings.repeat("Hello bob\n", 5), output);
  }
}
