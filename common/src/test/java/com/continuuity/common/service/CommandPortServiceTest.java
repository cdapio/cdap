package com.continuuity.common.service;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CommandPortServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(CommandPortServiceTest.class);

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
    final CommandPortService server = CommandPortService.builder("test")
                                                  .addCommandHandler("increment", "Increments a counter", handler)
                                                  .build();

    final CountDownLatch stopLatch = new CountDownLatch(1);
    Futures.addCallback(server.start(), new FutureCallback<Service.State>() {
      @Override
      public void onSuccess(Service.State result) {
        stopLatch.countDown();
      }

      @Override
      public void onFailure(Throwable t) {
        stopLatch.countDown();
      }
    });
    // wait a bit for service to start
    TimeUnit.SECONDS.sleep(1);

    try {
      for (int i = 0; i < 10; i++) {
        Socket clientSocket = new Socket("localhost", server.getPort());
        try {
          BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), "UTF-8"));
          BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), "UTF-8"));

          writer.write("increment");
          writer.newLine();
          writer.flush();

          String response = reader.readLine();
          Assert.assertEquals(i + 1, Integer.parseInt(response));
        } finally {
          clientSocket.close();
        }
      }

    } finally {
      server.stopAndWait();
    }

    Assert.assertEquals(10, handler.getCounter());
    Assert.assertTrue(stopLatch.await(3, TimeUnit.SECONDS));
    Assert.assertEquals(Service.State.TERMINATED, server.state());
  }
}
