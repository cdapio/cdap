/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataproc;

import co.cask.cdap.api.common.Bytes;
import com.google.common.base.Stopwatch;
import com.google.common.io.CharStreams;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * createSession takes about ~2.5 seconds
 * connecting a ChannelExec takes about 240ms
 * reading input from InputStream takes about 300ms
 */
public class SSHUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SSHUtils.class);

  public static String runCommand(SSHConfig sshConfig,
                                  String input) throws JSchException, IOException {
    Session session = createSession(sshConfig);
    try {
      return runCommand(session, input);
    } finally {
      session.disconnect();
    }
  }

  public static String runCommand(Session session,
                                  String input) throws JSchException, IOException {
    System.out.println("running command: " + input);
    Stopwatch sw = new Stopwatch().start();

    sw.reset().start();
    Channel channel = session.openChannel("exec");
    System.out.println("openChannel took: " + sw.elapsedMillis());
    ((ChannelExec) channel).setCommand(input);
    channel.setInputStream(null);
    ((ChannelExec) channel).setErrStream(System.err);
    InputStream in = channel.getInputStream();

    sw.reset().start();
    channel.connect();
    System.out.println("connect took: " + sw.elapsedMillis());
    sw.reset().start();
    String result = CharStreams.toString(new InputStreamReader(in, Charsets.UTF_8));
    System.out.println("reading all input took: " + sw.elapsedMillis());

    if (!channel.isClosed()) {
      System.out.println("Channel is not closed.");
    }
    System.out.println("exit-status: " + channel.getExitStatus());
    sw.reset().start();
    channel.disconnect();
    System.out.println("channel disconnect took: " + sw.elapsedMillis());
    sw.reset().start();
    return result;
  }

  public static String runCommand2(SSHConfig sshConfig,
                                   String input) throws JSchException, IOException, InterruptedException {
    System.out.println("running command: " + input);
    Session session = createSession(sshConfig);

    Channel channel = session.openChannel("shell");
    OutputStream ops = channel.getOutputStream();
    PrintStream ps = new PrintStream(ops);
    channel.connect();
    ps.println(input);
    ps.flush();
    ps.close();
    if (ps.checkError()) {
      //
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    channel.setOutputStream(baos);
    channel.connect();
    TimeUnit.SECONDS.sleep(1);
    String output = baos.toString("UTF-8");
    baos.close();
    session.disconnect();
    return output;
  }

  public static void scp(SSHConfig sshConfig, String localFile, String remoteDir) throws JSchException, IOException {
    // https://medium.com/@ldclakmal/scp-with-java-b7b7dbcdbc85
    Session session = createSession(sshConfig);
    scp(session, localFile, remoteDir);
    session.disconnect();
  }

  public static void scp(Session session, String localFile, String remoteDir) throws JSchException, IOException {
    LOG.info("Starting SCP from {} to {}", localFile, remoteDir);
    copyLocalToRemote(session, localFile, remoteDir);
    LOG.info("Finished SCP from {} to {}", localFile, remoteDir);
  }

  public static Session createSession(SSHConfig sshConfig) throws JSchException {
    Stopwatch sw = new Stopwatch().start();
    JSch jsch = new JSch();

//    if (privateKey != null) {
//      if (keyPassword != null) {
//        jsch.addIdentity(privateKey, keyPassword);
//      } else {
//        jsch.addIdentity(privateKey);
//      }
//    }

//    jsch.addIdentity("name", keyFilePath.getBytes("UTF-8"), null, null);


    jsch.addIdentity("name", Bytes.toBytes(sshConfig.privateKey), null, null);
    Session session = jsch.getSession(sshConfig.user, sshConfig.host, sshConfig.port);
    session.setConfig("StrictHostKeyChecking", "no");
    session.connect();
    System.out.println("createSession took: " + sw.elapsedMillis());
    return session;
  }

  private static void copyLocalToRemote(Session session, String from, String to) throws JSchException, IOException {
    boolean ptimestamp = true;

    // exec 'scp -t rfile' remotely
    String command = "scp " + (ptimestamp ? "-p" : "") + " -t " + to;
    Channel channel = session.openChannel("exec");
    ((ChannelExec) channel).setCommand(command);

    // get I/O streams for remote scp
    OutputStream out = channel.getOutputStream();
    InputStream in = channel.getInputStream();

    channel.connect();

    if (checkAck(in) != 0) {
      throw new IllegalStateException("Ack failed.");
    }

    File fromFile = new File(from);

    if (ptimestamp) {
      command = "T" + (fromFile.lastModified() / 1000) + " 0";
      // The access time should be sent here,
      // but it is not accessible with JavaAPI ;-<
      command += (" " + (fromFile.lastModified() / 1000) + " 0\n");
      out.write(command.getBytes());
      out.flush();
      if (checkAck(in) != 0) {
        throw new IllegalStateException("Ack failed.");
      }
    }

    // send "C0644 filesize filename", where filename should not include '/'
    long filesize = fromFile.length();
    command = "C0644 " + filesize + " ";
    if (from.lastIndexOf('/') > 0) {
      command += from.substring(from.lastIndexOf('/') + 1);
    } else {
      command += from;
    }

    command += "\n";
    out.write(command.getBytes());
    out.flush();

    if (checkAck(in) != 0) {
      throw new IllegalStateException("Ack failed.");
    }

    // send a content of lfile
    FileInputStream fis = new FileInputStream(from);
    byte[] buf = new byte[1024];
    while (true) {
      int len = fis.read(buf, 0, buf.length);
      if (len <= 0) {
        break;
      }
      out.write(buf, 0, len); //out.flush();
    }

    // send '\0'
    buf[0] = 0;
    out.write(buf, 0, 1);
    out.flush();

    if (checkAck(in) != 0) {
      throw new IllegalStateException("Ack failed.");
    }
    out.close();

    if (fis != null) {
      fis.close();
    }

    channel.disconnect();
  }

  private static int checkAck(InputStream in) throws IOException {
    int b = in.read();
    // b may be 0 for success,
    //          1 for error,
    //          2 for fatal error,
    //         -1
    if (b == 0) {
      return b;
    }
    if (b == -1) {
      return b;
    }

    if (b == 1 || b == 2) {
      StringBuffer sb = new StringBuffer();
      int c;
      do {
        c = in.read();
        sb.append((char) c);
      }
      while (c != '\n');
      if (b == 1) { // error
        System.out.print(sb.toString());
      }
      if (b == 2) { // fatal error
        System.out.print(sb.toString());
      }
    }
    return b;
  }

}
