/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.test.app;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.api.service.BasicService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import org.junit.Assert;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Test Application using StateStore.
 */
public class AppUsingStore extends AbstractApplication {

  @Override
  public void configure() {
    setName("AppUsingStore");
    setDescription("Application using StateStore");
    addService(new BasicService("NoOpService", new PingHandler()));
    addFlow(new StoreCheckFlow());
  }

  public static final class StoreCheckFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("StoreCheckFlow")
        .setDescription("Store, Emit, Check")
        .withFlowlets()
          .add("source", new SourceFlowlet())
          .add("sink", new SinkFlowlet())
        .connect()
          .from("source").to("sink")
        .build();
    }
  }

  private static final class SourceFlowlet extends AbstractFlowlet {
    private OutputEmitter<String> emitter;
    private int i = 0;

    @Tick(delay =  1L, unit = TimeUnit.SECONDS)
    void process() {
      Map<String, String> state = getContext().getState();
      if (state == null) {
        state = Maps.newHashMap();
      }
      state.put("key" + i, "value" + i);
      getContext().saveState(state);
      emitter.emit("key" + i);
      i++;
    }
  }

  private static final class SinkFlowlet extends AbstractFlowlet {
    private int i = 0;

    @ProcessInput
    void process(String key) {
      Assert.assertEquals("value" + i, getContext().getState().get(key));
      i++;
    }
  }

  public static final class PingHandler extends AbstractHttpServiceHandler {

    @Path("count")
    @POST
    public void incrCount(HttpServiceRequest request, HttpServiceResponder responder) {
      Map<String, String> state = getContext().getState();
      if (getContext().getState() == null) {
        state = Maps.newHashMap();
        state.put("call", Integer.toString(1));
        getContext().saveState(state);
      } else {
        String call = Integer.toString(Integer.valueOf(state.get("call")) + 1);
        state.put("call", call);
        getContext().saveState(state);
      }
      responder.sendStatus(200);
    }

    @Path("count")
    @GET
    public void getCount(HttpServiceRequest request, HttpServiceResponder responder) {
      responder.sendString(200, getContext().getState().get("call"), Charsets.UTF_8);
    }
  }
}
