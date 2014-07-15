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
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * AppWithServices with a DummyService for unit testing.
 */
public class AppWithServices extends AbstractApplication {
  private static final Logger LOG = LoggerFactory.getLogger(AppWithServices.class);

    @Override
    public void configure() {
      setName("WordCountApp");
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
        .setName("NoOpService")
        .withRunnable()
        .add(new DummyService())
        .noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  public static final class DummyService extends AbstractTwillRunnable {
    @Override
    public void run() {
      //No-op
      LOG.info("Runnable DummyService Started");
    }
    @Override
    public void stop() {
      LOG.info("Runnable DummyService Stopped");
    }
  }
}
