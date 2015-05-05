/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
   * @param namespace the namespace to which the input is deployed.
   * @param id the id of the input to deploy. If null, a default is used
   * @param input the input to the deployment pipeline
   * @return A future of the output of the deployment pipeline
   */
  ListenableFuture<O> deploy(Id.Namespace namespace, @Nullable String id, I input) throws Exception;
}
