/*
 * Copyright © 2017 Cask Data, Inc.
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

import HttpExecutorActions from 'components/HttpExecutor/store/HttpExecutorActions';
import HttpExecutorStore from 'components/HttpExecutor/store/HttpExecutorStore';
import { MyBlankPathApi } from 'api/blankpath';

export function execute() {
  let state = HttpExecutorStore.getState().http;

  let { method, path, body, headers } = state;

  let api;

  switch (method) {
    case 'GET':
      api = MyBlankPathApi.get;
      break;
    case 'PUT':
      api = MyBlankPathApi.put;
      break;
    case 'POST':
      api = MyBlankPathApi.post;
      break;
    case 'DELETE':
      api = MyBlankPathApi.delete;
      break;
  }

  // Creating Body
  let requestBody = null;
  if (['GET', 'DELETE'].indexOf(method) === -1 && body) {
    requestBody = body;
    try {
      requestBody = JSON.parse(body);
    } catch (e) {
      console.log('Request body is not of type JSON');
    }
  }

  let requestHeaders = {};

  if (headers.pairs && headers.pairs.length > 0) {
    headers.pairs.forEach((header) => {
      if (header.key.length > 0 && header.value.length > 0) {
        requestHeaders[header.key] = header.value;
      }
    });
  }

  api({ path }, requestBody, requestHeaders).subscribe(
    (res) => {
      HttpExecutorStore.dispatch({
        type: HttpExecutorActions.setResponse,
        payload: {
          response: res,
          statusCode: 200,
        },
      });
    },
    (err) => {
      HttpExecutorStore.dispatch({
        type: HttpExecutorActions.setResponse,
        payload: {
          response: err.data || err.response,
          statusCode: err.statusCode,
        },
      });
    }
  );
}
