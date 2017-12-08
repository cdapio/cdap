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

import {Subject} from 'rxjs/Subject';
const MyDataPrepApi = {
  explorerValues: {
    values: []
  },
  isError: false
};

MyDataPrepApi.generalGetter = function(property) {
  return function() {
    let subject = new Subject();
    setTimeout(() => {
      if (this.__isError) {
        subject.error(this[property]);
        return;
      }
      subject.next(this[property]);
    });
    return subject;
  }.bind(this);
};

MyDataPrepApi.explorer = MyDataPrepApi.generalGetter('explorerValues');
MyDataPrepApi.__setExplorerValues = function(values, isError) {
  this.__isError = isError;
  this.explorerValues = values;
};
export default MyDataPrepApi;
