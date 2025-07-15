/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.gson.Gson;
import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/preview-runner")
public class PreviewRunnerHttpHandlerInternal extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewRunnerHttpHandlerInternal.class);

  private static final Gson GSON = new Gson();

  /**
   * Holds the total number of requests that have been executed by this handler that should count
   * toward max allowed.
   */
  private final AtomicInteger runningRequestCount = new AtomicInteger(0);
  private final AtomicInteger requestProcessedCount = new AtomicInteger(0);
  private final CConfiguration cConf;
  private final int concurrentRequestLimit;

  public PreviewRunnerHttpHandlerInternal(CConfiguration cConf) {
    this.cConf = cConf;
    this.concurrentRequestLimit = 1;
  }

  @POST
  @Path("/run")
  public void run(FullHttpRequest request, HttpResponder responder) {
    LOG.info("sidhdirenge - Run called");
    responder.sendStatus(HttpResponseStatus.OK);
  }

}
