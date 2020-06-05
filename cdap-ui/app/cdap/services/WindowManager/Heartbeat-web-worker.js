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

const POLL_INTERVAL = 180000;
var intervalTimer = null;
onmessage = function(e) {
  if (e.data.timer === 'stop') {
    clearInterval(intervalTimer);
  }
  if (e.data.timer === 'start') {
    clearInterval(intervalTimer);
    intervalTimer = setInterval(() => {
      postMessage('');
    }, POLL_INTERVAL);
  }
}
