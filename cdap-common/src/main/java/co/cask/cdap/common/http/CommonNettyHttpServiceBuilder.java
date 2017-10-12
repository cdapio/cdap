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
package co.cask.cdap.common.http;

import co.cask.cdap.common.HttpExceptionHandler;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.http.ChannelPipelineModifier;
import co.cask.http.NettyHttpService;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.EventExecutor;

/**
 * Provides a {@link co.cask.http.NettyHttpService.Builder} that has common settings built-in.
 */
public class CommonNettyHttpServiceBuilder extends NettyHttpService.Builder {

  public CommonNettyHttpServiceBuilder(CConfiguration cConf, String serviceName) {
    super(serviceName);

    if (cConf.getBoolean(Constants.Security.ENABLED)) {
      setChannelPipelineModifier(new ChannelPipelineModifier() {
        @Override
        public void modify(ChannelPipeline pipeline) {
          // Adds the AuthenticationChannelHandler before the dispatcher, using the same
          // EventExecutor to make sure they get invoked from the same thread
          // This is needed before we use a InheritableThreadLocal in SecurityRequestContext
          // to remember the user id.
          EventExecutor executor = pipeline.context("dispatcher").executor();
          pipeline.addBefore(executor, "dispatcher", "authenticator", new AuthenticationChannelHandler());
        }
      });
    }
    this.setExceptionHandler(new HttpExceptionHandler());
  }
}
