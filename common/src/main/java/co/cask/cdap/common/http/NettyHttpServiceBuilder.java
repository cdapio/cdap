/*
 * Copyright 2014 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.http.NettyHttpService;
import com.google.common.base.Function;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.jboss.netty.channel.ChannelPipeline;

/**
 * Provides a {@link NettyHttpService.Builder} that has common configuration built-in.
 */
public class NettyHttpServiceBuilder implements Provider<NettyHttpService.Builder> {

  private final CConfiguration configuration;
  private final AuthenticationChannelHandler authenticationChannelHandler;

  @Inject
  public NettyHttpServiceBuilder(CConfiguration configuration,
                                 AuthenticationChannelHandler authenticationChannelHandler) {
    this.configuration = configuration;
    this.authenticationChannelHandler = authenticationChannelHandler;
  }

  public NettyHttpServiceBuilder() {
    this.configuration = CConfiguration.create();
    this.authenticationChannelHandler = new AuthenticationChannelHandler();
  }

  @Override
  public NettyHttpService.Builder get() {
    NettyHttpService.Builder builder = NettyHttpService.builder();
    if (configuration.getBoolean(Constants.Security.CFG_SECURITY_AUTHORIZATION_ENABLED)) {
      builder.modifyChannelPipeline(new Function<ChannelPipeline, ChannelPipeline>() {
        @Override
        public ChannelPipeline apply(ChannelPipeline input) {
          input.addAfter("decoder", AuthenticationChannelHandler.HANDLER_NAME, authenticationChannelHandler);
          return input;
        }
      });
    }
    return builder;
  }
}
