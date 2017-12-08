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
import {Subject} from 'rxjs/Subject';

class Socket {
  constructor() {
    this.buffer = [];
    this.observable = new Subject();
    this.timeout = null;

    this.init();
  }

  init(attempt) {
    attempt = attempt || 1;
    clearTimeout(this.timeout);

    this.socket = new SockJS('/_sock');

    this.socket.onopen = () => {
      // Buffering request while Socket is still starting up
      this.buffer.forEach((req) => {
        this._doSend(req);
      });
      this.buffer = [];
    };

    this.socket.onmessage = (event) => {
      try {
        let data = JSON.parse(event.data);
        if (window.CDAP_CONFIG.cdap.uiDebugEnabled) {
          console.groupCollapsed('response: ' + data.resource.url);
          console.log(data.resource);
          console.log(data.response);
          console.groupEnd();
        }
        this.observable.next(data);
      } catch (e) {
        console.log('error', e);
      }
    };

    // Need to implement the reconnect function
    this.socket.onclose = () => {
      // reconnect with exponential backoff
      let delay = Math.max(500, Math.round(
        (Math.random() + 1) * 500 * Math.pow(2, attempt)
      ));

      this.timeout = setTimeout(() => {
        this.init(attempt++);
      }, delay);
    };
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
    if (window.CDAP_CONFIG.cdap.uiDebugEnabled) {
      console.groupCollapsed('request: ' + obj.resource.url);
      console.log(obj.resource);
      console.groupEnd();
    }
    this.socket.send(JSON.stringify(obj));
  }
}

export default new Socket();
