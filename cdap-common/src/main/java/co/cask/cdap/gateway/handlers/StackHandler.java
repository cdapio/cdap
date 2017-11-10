/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.Constants;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Returns information about threads, including their stack traces.
 */
public class StackHandler extends AbstractHttpHandler {

  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

  @Path(Constants.Gateway.API_VERSION_3 + "/system/services/{service-name}/stacks")
  @GET
  public void stacks(HttpRequest request, HttpResponder responder,
                     @PathParam("service-name") String serviceName,
                     @QueryParam("depth") @DefaultValue("20") int depth) {
    StringWriter stringWriter = new StringWriter();
    getThreadInfo(new PrintWriter(stringWriter), serviceName, depth);
    responder.sendString(HttpResponseStatus.OK, stringWriter.toString());
  }

  /**
   * Print all of the thread's information and stack traces.
   */
  private synchronized void getThreadInfo(PrintWriter stream, String serviceName, int depth) {
    boolean contention = THREAD_MX_BEAN.isThreadContentionMonitoringEnabled();
    long[] threadIds = THREAD_MX_BEAN.getAllThreadIds();
    println(stream, "Process Thread Dump: %s", serviceName);
    println(stream, "%s active threads", threadIds.length);
    for (long tid : threadIds) {
      ThreadInfo info = THREAD_MX_BEAN.getThreadInfo(tid, depth);
      if (info == null) {
        stream.println("  Inactive");
        continue;
      }
      println(stream, "Thread %s",
              getTaskName(info.getThreadId(), info.getThreadName()) + ":");
      Thread.State state = info.getThreadState();
      println(stream, "  State: %s", state);
      println(stream, "  Blocked count: %s", info.getBlockedCount());
      println(stream, "  Waited count: %s", info.getWaitedCount());
      if (contention) {
        println(stream, "  Blocked time: %s", info.getBlockedTime());
        println(stream, "  Waited time: %s", info.getWaitedTime());
      }
      if (state == Thread.State.WAITING) {
        println(stream, "  Waiting on %s", info.getLockName());
      } else if (state == Thread.State.BLOCKED) {
        println(stream, "  Blocked on %s", info.getLockName());
        println(stream, "  Blocked by %s",
                         getTaskName(info.getLockOwnerId(), info.getLockOwnerName()));
      }
      println(stream, "  Stack:");
      for (StackTraceElement frame : info.getStackTrace()) {
        println(stream, "    %s", frame.toString());
      }
    }
    stream.flush();
  }

  private void println(PrintWriter stream, String format, Object... args) {
    stream.printf(format, args).println();
  }

  private String getTaskName(long id, String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }
}
