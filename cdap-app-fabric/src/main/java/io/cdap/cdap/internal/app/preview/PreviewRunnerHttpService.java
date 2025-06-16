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

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Preview;
import io.cdap.cdap.common.conf.Constants.Service;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.http.ChannelPipelineModifier;
import io.cdap.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpContentDecompressor;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreviewRunnerHttpService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewRunnerHttpService.class);

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancelDiscovery;
  private InetSocketAddress bindAddress;

  @Inject
  PreviewRunnerHttpService(CConfiguration cConf, SConfiguration sConf,
      DiscoveryService discoveryService) {
    this.discoveryService = discoveryService;

//    // set workdir location in cConf
//    // workdir location is unique per task worker and accessible via env var
//    String workDir = System.getenv("CDAP_LOCAL_DIR");
//    if (workDir != null) {
//      cConf.set(TaskWorker.WORK_DIR, workDir);
//    }

    NettyHttpService.Builder builder = NettyHttpService.builder(
            Service.PREVIEW_RUNNER_HTTP).setHost(cConf.get(Preview.RUNNER_ADDRESS))
        .setPort(cConf.getInt(Preview.RUNNER_PORT))
        .setExecThreadPoolSize(cConf.getInt(Constants.Preview.EXEC_THREADS))
        .setBossThreadPoolSize(cConf.getInt(Constants.Preview.BOSS_THREADS))
        .setWorkerThreadPoolSize(cConf.getInt(Constants.Preview.WORKER_THREADS))
        .setChannelPipelineModifier(new ChannelPipelineModifier() {
          @Override
          public void modify(ChannelPipeline pipeline) {
            pipeline.addAfter("compressor", "decompressor", new HttpContentDecompressor());
          }
        }).setHttpHandlers(new PreviewRunnerHttpHandlerInternal(cConf));

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }
    this.httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("sidhdirenge - Starting PreviewRunnerHttpService");
    httpService.start();
    bindAddress = httpService.getBindAddress();
    cancelDiscovery = discoveryService.register(ResolvingDiscoverable.of(
        URIScheme.createDiscoverable(Service.PREVIEW_RUNNER_HTTP, httpService)));
    LOG.debug("sidhdirenge - Starting PreviewRunnerHttpService has completed");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("sidhdirenge - Shutting down PreviewRunnerHttpService");
    httpService.stop(1, 2, TimeUnit.SECONDS);
    cancelDiscovery.cancel();
    LOG.debug("sidhdirenge - Shutting down PreviewRunnerHttpService has completed");
  }

  private void stopService(String className) {
    /*
     * TODO: Expand this logic such that
     * based on number of requests per particular class,
     * the service gets stopped.
     */
    stop();
  }
}