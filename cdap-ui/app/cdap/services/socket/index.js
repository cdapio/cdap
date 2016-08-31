/*
 * Copyright © 2016 Cask Data, Inc.
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

import SockJS from 'sockjs-client';
import Rx from 'rx';

class Socket {
  constructor() {
    this.socket = new SockJS('/_sock');
    this.buffer = [];

    this.socket.onopen = () => {

      // Buffering request while Socket is still starting up
      this.buffer.forEach((req) => {
        this._doSend(req);
      });
      this.buffer = [];
    };

    this.observable = Rx.Observable.create( (obs) => {
      this.socket.onmessage = (event) => {
        try {
          let data = JSON.parse(event.data);
          obs.onNext(data);
        } catch (e) {
          console.log('error', e);
        }
      };
    });

    // Need to implement the reconnect function
    // this.socket.onclose
  }

  getObservable() {
    return this.observable;
  }

  send(obj) {
    if (!this.socket.readyState) {
      this.buffer.push(obj);
      return false;
    }

    this._doSend(obj);
    return true;
  }

  _doSend(obj) {
    this.socket.send(JSON.stringify(obj));
  }
}

export default new Socket();
