/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.k8s.runtime;

import io.kubernetes.client.ApiCallback;
import io.kubernetes.client.ApiException;

import java.util.List;
import java.util.Map;

/**
 * Allows more succinct creation of ApiCallback.
 *
 * @param <T> The return type
 */
class ApiCallbackAdapter<T> implements ApiCallback<T> {

  @Override
  public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
    // no-op
  }

  @Override
  public void onSuccess(T result, int statusCode, Map<String, List<String>> responseHeaders) {
    // no-op
  }

  @Override
  public void onUploadProgress(long bytesWritten, long contentLength, boolean done) {
    // no-op
  }

  @Override
  public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {
    // no-op
  }
}
