/*
 * Copyright © 2014-2024 Cask Data, Inc.
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

package io.cdap.cdap.common.service;

import com.google.common.util.concurrent.Service;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link CommandPortService}.
 */
public class CommandPortServiceTest {

  private static final class IncrementCommandHandler implements CommandPortService.CommandHandler {
    private int counter;

    public int getCounter() {
      return counter;
    }

    @Override
    public void handle(BufferedWriter respondWriter) throws IOException {
      counter++;
      respondWriter.write(String.format("%d", counter));
      respondWriter.newLine();
    }
  }

  @Test
  public void testCommandPortServer() throws Exception {
    IncrementCommandHandler handler = new IncrementCommandHandler();
    CommandPortService server = CommandPortService.builder("test")
        .addCommandHandler("increment", "Increments a counter", handler)
        .build();

    try {
      server.getPort();
      Assert.fail("Expected IllegalStateException before service is running");
    } catch (IllegalStateException expected) {
      // expected
    }

    server.startAsync().awaitRunning();
    try {
      for (int i = 0; i < 10; i++) {
        List<String> lines = sendCommand(server.getPort(), "increment");
        Assert.assertEquals(1, lines.size());
        Assert.assertEquals(i + 1, Integer.parseInt(lines.get(0)));
      }
    } finally {
      server.stopAsync().awaitTerminated();
    }

    Assert.assertEquals(10, handler.getCounter());
    Assert.assertEquals(Service.State.TERMINATED, server.state());

    try {
      server.getPort();
      Assert.fail("Expected IllegalStateException after service stopped");
    } catch (IllegalStateException expected) {
      // expected
    }
  }

  @Test
  public void testHelpUnknownAndFailingCommandHandlers() throws Exception {
    IncrementCommandHandler incrementHandler = new IncrementCommandHandler();
    CommandPortService server = CommandPortService.builder("myservice")
        .addCommandHandler("increment", "Increments a counter", incrementHandler)
        .addCommandHandler("boom", "Throws an error", writer -> {
          throw new RuntimeException("handler boom");
        })
        .build();

    server.startAsync().awaitRunning();
    try {
      int port = server.getPort();
      List<String> helpLines = sendCommand(port, "help");
      Assert.assertTrue(helpLines.get(0).contains("Help for myservice command port service"));
      Assert.assertTrue(helpLines.stream().anyMatch(l -> l.contains("increment : Increments a counter")));

      // Unknown command and throwing handler should not crash the server loop.
      Assert.assertTrue(sendCommand(port, "unknown_cmd").isEmpty());
      Assert.assertTrue(sendCommand(port, "boom").isEmpty());

      List<String> afterBoom = sendCommand(port, "increment");
      Assert.assertEquals("1", afterBoom.get(0));
    } finally {
      server.stopAsync().awaitTerminated();
    }
    Assert.assertEquals(Service.State.TERMINATED, server.state());
  }

  @Test
  public void testCustomHelpAndPortBindFailure() throws Exception {
    CommandPortService customHelpServer = CommandPortService.builder("custom")
        .addCommandHandler("help", "Custom help", writer -> {
          writer.write("custom-help-output");
          writer.newLine();
        })
        .build();

    customHelpServer.startAsync().awaitRunning();
    try {
      List<String> lines = sendCommand(customHelpServer.getPort(), "help");
      Assert.assertEquals(1, lines.size());
      Assert.assertEquals("custom-help-output", lines.get(0));
    } finally {
      customHelpServer.stopAsync().awaitTerminated();
    }

    try (ServerSocket occupied = new ServerSocket(0, 0, InetAddress.getByName("localhost"))) {
      CommandPortService conflicting = CommandPortService.builder("conflict")
          .setPort(occupied.getLocalPort())
          .build();
      try {
        conflicting.startAsync().awaitRunning();
        Assert.fail("Expected port bind failure");
      } catch (RuntimeException expected) {
        Assert.assertEquals(Service.State.FAILED, conflicting.state());
      }
    }
  }

  private static List<String> sendCommand(int port, String command) throws IOException {
    try (Socket clientSocket = new Socket("localhost", port);
         BufferedWriter writer = new BufferedWriter(
             new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8));
         BufferedReader reader = new BufferedReader(
             new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8))) {
      writer.write(command);
      writer.newLine();
      writer.flush();

      List<String> lines = new ArrayList<>();
      String line = reader.readLine();
      while (line != null) {
        lines.add(line);
        line = reader.readLine();
      }
      return lines;
    }
  }
}
