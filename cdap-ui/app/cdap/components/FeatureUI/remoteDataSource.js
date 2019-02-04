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

import { Observable } from 'rxjs/Observable';
import { REMOTE_IP } from './config';

class RemoteDataSource {
  request(reqObj) {
    return Observable.fromPromise(fetch(this.getFetchUrl(reqObj), this.getFetchObject(reqObj)).then(res => res.json()));
  }

  poll(reqObj) {
    return Observable.fromPromise(fetch(reqObj._cdapPath).then(res => res.json()));
  }

  getFetchUrl(request) {
    return REMOTE_IP + "/v3" + request._cdapPath;
  }

  getFetchObject(request) {
    let fetchObject = {};
    switch (request.method) {
      case "POST":
        fetchObject = {
          method: 'POST',
          body: JSON.stringify(request.body)
        };
        break;
      case "DELETE":
        fetchObject = {
          method: 'DELETE',
          body: ""
        };
        break;
      case "GET":
        fetchObject = {};
        break;
      case "PUT":
        fetchObject = {
          method: 'PUT',
          body: JSON.stringify(request.body)
        };
        break;
    }
    return fetchObject;
  }
}
const remoteDataSource = new RemoteDataSource();
export default remoteDataSource;


