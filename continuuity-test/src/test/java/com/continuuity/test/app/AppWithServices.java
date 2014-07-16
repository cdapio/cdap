/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.test.app;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.google.common.base.Throwables;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.common.Cancellable;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * AppWithServices with a DummyService for unit testing.
 */
public class AppWithServices extends AbstractApplication {

    @Override
    public void configure() {
      setName("AppWithServices");
      addStream(new Stream("text"));
      addProcedure(new NoOpProcedure());
      addService(new TwillService());
   }


  public static final class NoOpProcedure extends AbstractProcedure {

    @Handle("ping")
    public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      responder.sendJson(ProcedureResponse.Code.SUCCESS, "OK");
    }

  }

  public static class TwillService implements TwillApplication {
    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("ServerService")
        .withRunnable()
        .add(new ServerService(),
             ResourceSpecification.Builder.with()
               .setVirtualCores(1)
               .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
               .setInstances(2)
               .build())
        .noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  public static final class ServerService extends AbstractTwillRunnable {

    private Cancellable discoveryCancel;
    private ServerSocket serverSocket;

    @Override
    public void initialize(final TwillContext context) {
      super.initialize(context);

      try {
        serverSocket = new ServerSocket(0);
        context.electLeader("server", new ElectionHandler() {
          @Override
          public void leader() {
            discoveryCancel = context.announce("server", serverSocket.getLocalPort());
          }

          @Override
          public void follower() {
            if (discoveryCancel != null) {
              discoveryCancel.cancel();
            }
          }
        });
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void run() {
      try {
        // Block for an incoming connection
        Socket socket = serverSocket.accept();
        socket.close();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void stop() {
      try {
        serverSocket.close();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void destroy() {
      try {
        serverSocket.close();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
