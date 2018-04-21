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

package co.cask.cdap.common.ssh;

import co.cask.cdap.runtime.spi.ssh.SSHProcess;
import co.cask.cdap.runtime.spi.ssh.SSHSession;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * The default implementation of {@link SSHSession} that uses {@link JSch} library.
 */
public class DefaultSSHSession implements SSHSession {

  private final Session session;

  /**
   * Creates a new instance based on the given {@link SSHConfig}.
   *
   * @param config configurations about the ssh session
   * @throws IOException if failed to connect according to the configuration
   */
  public DefaultSSHSession(SSHConfig config) throws IOException {
    JSch jsch = new JSch();

    try {
      jsch.addIdentity(config.getUser(), config.getPrivateKey(), null, null);
      Session session = jsch.getSession(config.getUser(), config.getHost(), config.getPort());
      session.setConfig("StrictHostKeyChecking", "no");

      for (Map.Entry<String, String> entry : config.getConfigs().entrySet()) {
        session.setConfig(entry.getKey(), entry.getValue());
      }

      session.connect();
      this.session = session;
    } catch (JSchException e) {
      throw new IOException(e);
    }
  }

  @Override
  public SSHProcess execute(List<String> commands) throws IOException {
    try {
      Channel channel = session.openChannel("exec");

      try {
        ChannelExec channelExec = (ChannelExec) channel;
        // Should get the stream before connecting.
        // Otherwise JSch will write the output to some default stream, causing data missing from
        // the InputStream that acquired later.
        SSHProcess process = new DefaultSSHProcess(channelExec, channelExec.getOutputStream(),
                                                   channelExec.getInputStream(), channelExec.getErrStream());
        channelExec.setCommand(commands.stream().collect(Collectors.joining(";")));
        channelExec.connect();

        return process;
      } catch (Exception e) {
        channel.disconnect();
        throw e;
      }

    } catch (JSchException e) {
      throw new IOException(e);
    }
  }

  @Override
  public String executeAndWait(List<String> commands) throws IOException {
    SSHProcess process = execute(commands);

    // Reading will be blocked until the process finished
    String out = CharStreams.toString(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
    String err = CharStreams.toString(new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8));

    // Await uninterruptedly
    boolean interrupted = false;
    try {
      while (true) {
        try {
          int exitCode = process.waitFor();
          if (exitCode != 0) {
            throw new RuntimeException("Commands execution failed with exit code (" + exitCode + ") Commands: " +
                                         commands + ", Output: " + out + " Error: " + err);
          }
          return out;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void copy(Path sourceFile, String targetPath) throws IOException {
    @SuppressWarnings("OctalInteger")
    int permissions = 0644;
    BasicFileAttributes attrs;
    try {
      // Try to read attributes. The PosixFileAttributes will fail under Windows,
      // which we'll use BasicFileAttributes instead and with permission default to 0644
      PosixFileAttributes posixAttrs = Files.readAttributes(sourceFile, PosixFileAttributes.class);
      permissions = resolvePermissions(posixAttrs.permissions());
      attrs = posixAttrs;
    } catch (Exception e) {
      attrs = Files.readAttributes(sourceFile, BasicFileAttributes.class);
    }

    // Copy the file
    try (InputStream is = Files.newInputStream(sourceFile)) {
      copy(is, targetPath, sourceFile.getFileName().toString(), attrs.size(), permissions,
           attrs.lastAccessTime().toMillis(), attrs.lastModifiedTime().toMillis());
    }
  }

  @Override
  public void copy(InputStream input, String targetPath, String targetName, long size, int permission,
                   @Nullable Long lastAccessTime, @Nullable Long lastModifiedTime) throws IOException {

    boolean preserveTimestamp = lastAccessTime != null && lastModifiedTime != null;
    String command = "scp " + (preserveTimestamp ? "-p" : "") + " -t " + targetPath;

    try {
      Channel channel = session.openChannel("exec");
      ((ChannelExec) channel).setCommand(command);

      channel.connect();

      // get I/O streams for remote scp
      try (OutputStream out = channel.getOutputStream();
           InputStream in = channel.getInputStream()) {

        checkAck(in);

        if (preserveTimestamp) {
          command = String.format("T%d 0 %d 0\n", TimeUnit.MILLISECONDS.toSeconds(lastModifiedTime),
                                  TimeUnit.MILLISECONDS.toSeconds(lastAccessTime));
          out.write(command.getBytes(StandardCharsets.UTF_8));
          out.flush();
          checkAck(in);
        }

        // send "C[4 digits permissions] [file_size] filename", where filename should not include '/'
        command = String.format("C%04o %d %s\n", permission, size, targetName);
        out.write(command.getBytes(StandardCharsets.UTF_8));
        out.flush();
        checkAck(in);

        // send a content of file
        ByteStreams.copy(input, out);

        // send '\0'
        out.write(0);
        out.flush();
        checkAck(in);
      } finally {
        channel.disconnect();
      }
    } catch (JSchException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() {
    session.disconnect();
  }

  /**
   * Validates the ack from the remote server for the SCP command.
   */
  private void checkAck(InputStream in) throws IOException {
    int b = in.read();
    // b may be 0 for success,
    //          1 for error,
    //          2 for fatal error,
    //         -1
    if (b == 0) {
      return;
    }

    if (b == -1) {
      throw new IllegalStateException("Ack failed");
    }

    if (b == 1 || b == 2) {
      StringBuilder sb = new StringBuilder();
      int c;
      do {
        c = in.read();
        sb.append((char) c);
      } while (c != '\n');


      if (b == 1) { // error
        throw new IOException("Error in ack: " + sb.toString());
      }
      // fatal error
      throw new IOException("Fatal error in ack " + sb.toString());
    }
  }

  private int resolvePermissions(Set<PosixFilePermission> posixPermissions) {
    int permissions = 0;
    if (posixPermissions.contains(PosixFilePermission.OWNER_READ)) {
      permissions |= 1 << 8;
    }
    if (posixPermissions.contains(PosixFilePermission.OWNER_WRITE)) {
      permissions |= 1 << 7;
    }
    if (posixPermissions.contains(PosixFilePermission.OWNER_EXECUTE)) {
      permissions |= 1 << 6;
    }

    if (posixPermissions.contains(PosixFilePermission.GROUP_READ)) {
      permissions |= 1 << 5;
    }
    if (posixPermissions.contains(PosixFilePermission.GROUP_WRITE)) {
      permissions |= 1 << 4;
    }
    if (posixPermissions.contains(PosixFilePermission.GROUP_EXECUTE)) {
      permissions |= 1 << 3;
    }

    if (posixPermissions.contains(PosixFilePermission.OTHERS_READ)) {
      permissions |= 1 << 2;
    }
    if (posixPermissions.contains(PosixFilePermission.OTHERS_WRITE)) {
      permissions |= 1 << 1;
    }
    if (posixPermissions.contains(PosixFilePermission.OTHERS_EXECUTE)) {
      permissions |= 1;
    }
    return permissions;
  }
}
