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

package co.cask.cdap.common.service;

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
