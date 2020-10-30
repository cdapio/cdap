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

import ee from 'event-emitter';
import 'whatwg-fetch';
import ifvisible from 'ifvisible.js';
import { objectQuery } from 'services/helpers';
const WINDOW_ON_BLUR = 'WINDOW_BLUR_EVENT';
const WINDOW_ON_FOCUS = 'WINDOW_FOCUS_EVENT';

class WindowManager {
  public eventemitter = ee(ee);
  private worker = null;
  private initWorker = () => {
    if (objectQuery(window, 'CDAP_CONFIG', 'cdap', 'proxyBaseUrl')) {
      this.worker = new Worker(`/cdap_assets/web-workers/Heartbeat-web-worker.js?q=${Date.now()}`);
      this.worker.onmessage = () => {
        this.reloadImage();
      };
    }
  };
  constructor() {
    if (window.parent.Cypress) {
      return;
    }
    if (ifvisible.now('hidden')) {
      this.onBlurEventHandler();
    }
    ifvisible.on('idle', () => {
      if (window.parent.Cypress) {
        return;
      }
      this.onBlurEventHandler();
    });
    ifvisible.on('wakeup', () => {
      if (window.parent.Cypress) {
        return;
      }
      this.onFocusHandler();
    });
    ifvisible.setIdleDuration(300);
    this.initWorker();
  }
  /**
   * We need this ping going from the browser to nodejs server.
   * This serves in enviornments where we need the browser make a request
   * before the auth session expires.
   *
   * This is useful when the user is no longer active in the tab
   * for sometime and switches back. Without this the UI will be unable
   * to reach backend and will show UI node server is down, but on refresh will
   * function just fine.
   */
  public reloadImage = () => {
    const alreadyExistingImage = document.getElementById('heartbeat-img-id');
    if (alreadyExistingImage) {
      alreadyExistingImage.parentNode.removeChild(alreadyExistingImage);
    }

    const newImage = document.createElement('img');
    newImage.src = `${location.origin}/cdap_assets/img/heartbeat.png?time=${Date.now()}`;
    newImage.id = 'heartbeat-img-id';
    newImage.style.width = '1px';
    newImage.style.position = 'absolute';
    newImage.style.left = '-3000px';
    document.body.appendChild(newImage);
  };

  public onBlurEventHandler = () => {
    if (this.worker) {
      this.worker.postMessage({
        timer: 'start',
      });
    }
    this.eventemitter.emit(WINDOW_ON_BLUR);
  };

  public onFocusHandler = () => {
    if (this.worker) {
      this.worker.postMessage({
        timer: 'stop',
      });
    }
    this.eventemitter.emit(WINDOW_ON_FOCUS);
  };

  public isWindowActive = () => {
    if (window.parent.Cypress) {
      return true;
    }
    return ifvisible.now('active');
  };
}

export default new WindowManager();
export { WINDOW_ON_BLUR, WINDOW_ON_FOCUS };
