/*
 * Copyright © 2019 Cask Data, Inc.
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
const WINDOW_ON_BLUR = 'WINDOW_BLUR_EVENT';
const WINDON_ON_FOCUS = 'WINDOW_FOCUS_EVENT';

class WindowManager {
  public eventemitter = ee(ee);
  constructor() {
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible') {
        this.onFocusHandler();
      } else {
        this.onBlurEventHandler();
      }
    });
  }

  public onBlurEventHandler = () => {
    this.eventemitter.emit(WINDOW_ON_BLUR);
  };

  public onFocusHandler = () => {
    this.eventemitter.emit(WINDON_ON_FOCUS);
  };
}

export default new WindowManager();
export { WINDOW_ON_BLUR, WINDON_ON_FOCUS };
