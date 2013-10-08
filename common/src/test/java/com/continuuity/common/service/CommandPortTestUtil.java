package com.continuuity.common.service;

import org.junit.Assert;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.Socket;

/**
 *
 */
public final class CommandPortTestUtil {
  public static void verifyCommandPort(int port, String handler, String expectedResponse) throws Exception {
    Socket clientSocket = new Socket("localhost", port);
    try {
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream(), "UTF-8"));
      BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), "UTF-8"));

      writer.write(handler);
      writer.newLine();
      writer.flush();

      String response = reader.readLine();
      Assert.assertEquals(expectedResponse, response);
    } finally {
      clientSocket.close();
    }
  }

  public static void verifyCommandPortDown(int port) throws Exception {
    try {
      new Socket("localhost", port);
      Assert.fail("MUST NOT be up");
    } catch (ConnectException e) {
      // expected
    }
  }
}
