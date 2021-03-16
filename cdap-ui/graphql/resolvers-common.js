/*
 * Copyright © 2019-2020 Cask Data, Inc.
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

import request from 'request';
import { ApolloError } from 'apollo-server';

export function getGETRequestOptions() {
  return {
    method: 'GET',
    json: true,
  };
}

export function getPOSTRequestOptions() {
  return {
    method: 'POST',
    json: true,
  };
}

export function requestPromiseWrapper(options, { auth: token, userIdProperty, userIdValue }, bodyModifiersFn, errorModifiersFn) {
  if (token) {
    options.headers = {
      Authorization: token,
    };
  }

  if (userIdProperty) {
    options.headers = options.headers || {};
    options.headers[userIdProperty] = userIdValue;
  }

  return new Promise((resolve, reject) => {
    request(options, (err, response, body) => {
      const statusCode = response.statusCode;
      if (err) {
        let exception;
        if (typeof errorModifiersFn === 'function') {
          exception = errorModifiersFn(err, statusCode ? statusCode.toString() : '500');
        } else {
          exception = new ApolloError(err, statusCode ? statusCode.toString() : '500');
        }
        return reject(exception);
      }

      
      if (typeof statusCode === 'undefined' || statusCode != 200) {
        let error;
        if (typeof errorModifiersFn === 'function') {
          error = errorModifiersFn(body, statusCode.toString());
        } else {
          error = new ApolloError(body, statusCode.toString());
        }
        return reject(error);
      }

      let resultBody = body;
      if (typeof bodyModifiersFn === 'function') {
        resultBody = bodyModifiersFn(body);
      }

      return resolve(resultBody);
    });
  });
}
