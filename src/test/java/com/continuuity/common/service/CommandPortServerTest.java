package com.continuuity.common.service;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 *
 */
public class CommandPortServerTest {
  private static final Logger Log = LoggerFactory.getLogger(CommandPortServerTest.class);

  private class IncrementCommand implements CommandPortServer.CommandListener {
    private int counter;

    public IncrementCommand() {
      counter = 0;
    }

    public int getCounter() {
      return counter;
    }

    @Override
    public String act() {
      counter = counter + 1;
      return String.format("Incremented value %d", counter);
    }
  }

  @Test
  public void testCommandPortServer() throws Exception {
    final IncrementCommand incrementCommand = new IncrementCommand();
    final CommandPortServer server = new CommandPortServer("test");
    server.addListener("increment", "Increments a counter", incrementCommand);
    int port = server.getPort();

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          server.serve();
        } catch (CommandPortServer.CommandPortException e) {
          e.printStackTrace();
        }
      }
    });
    thread.start();

    /** Now connect client on the port server is running on */
    try {
      Socket clientSocket = new Socket("localhost", port);
      DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
      BufferedReader inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));;
      outToServer.writeBytes("increment\n");
      outToServer.flush();
      String response = inFromServer.readLine();
      Log.info("Response from server " + response);
      clientSocket.close();
    } catch (IOException e) {
      throw e;
    }
    server.stop();
    Assert.assertTrue(incrementCommand.getCounter() == 1);
  }
}
