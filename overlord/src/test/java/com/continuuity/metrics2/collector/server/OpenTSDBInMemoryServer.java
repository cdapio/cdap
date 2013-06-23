package com.continuuity.metrics2.collector.server;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;

/**
 * Simple implementation of TCP server that mocks openTSDB.
 */
public final class OpenTSDBInMemoryServer implements Runnable {
  private static Logger Log = LoggerFactory.getLogger(
    OpenTSDBInMemoryServer.class
  );
  private final ServerSocket server;
  private volatile boolean running = false;
  private List<String> commands = Lists.newArrayList();

  public OpenTSDBInMemoryServer(int port) throws IOException {
    this.server = new ServerSocket(port);
    Log.debug("Starting OpenTSDBinMemoryServer on port {}", port);
    this.server.setSoTimeout(5000); // set timeout to 100 milliseconds.
    this.running = true;
  }

  public int getCommandCount() {
    return commands.size();
  }

  public void stop() {
    this.running = false;
  }

  @Override
  public void run() {
    while(running) {
      try {
        System.out.println("Waiting for client(s) to connect.");
        Socket connected = server.accept();
        BufferedReader fromClient =
          new BufferedReader(new InputStreamReader(connected.getInputStream()));
        System.out.println("Connected...");
        String command = fromClient.readLine();
        System.out.println("Read line : " + command);

        connected.close();
        commands.add(command);
      } catch (SocketTimeoutException e) {
        // Received timeout, we will check if we are supposed to be still
        // running. If not then we exit.
        if(! running) {
          break;
        }
      } catch (IOException e) {
        Log.error(e.getMessage());
        break;
      }
    }
    try {
      server.close();
    } catch (IOException e) {
      Log.error("Error while closing server socket. Reason : {}",
                e.getMessage());
    }
  }
}
