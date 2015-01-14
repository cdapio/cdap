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

package co.cask.cdap.app.deploy;

import co.cask.cdap.proto.Id;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;

/**
 * Interface to represent deployment manager.
 *
 * @param <I> Input type.
 * @param <O> Output type.
 */
public interface Manager<I, O> {

  /**
   * Executes a pipeline for deploying an input.
   *
   * @param id account id to which the archive is deployed.
   * @param appId application id to be used to override app name provided by app spec. If null, name of app spec is used
   * @param input the input to the deployment pipeline
   * @return A future of Application with Programs.
   */
  ListenableFuture<O> deploy(Id.Namespace id, @Nullable String appId, I input) throws Exception;
}
