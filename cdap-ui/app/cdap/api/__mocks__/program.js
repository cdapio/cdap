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

/*
  TODO: This is just a stub(mock) for jest to not invoke the actual socket connection.
  This needs to be exported as a singleton class. Will do when we actually need to mock a function.
*/
import Rx from 'rx';

const MyProgramApi = {
  runRecords: [],
  status: {},
  __isError: false
};

MyProgramApi.setRunRecords = function(records, isError) {
  this.runRecords = records;
  this.__isError = isError;
};
MyProgramApi.setProgramStatus = function(status, isError) {
  this.status = status;
  this.__isError = isError;
};
MyProgramApi.generalGetter = function(property) {
  return function() {
    let subject = new Rx.Subject();
    setTimeout(() => {
      if (this.__isError) {
        subject.onError(this[property]);
        return;
      }
      subject.onNext(this[property]);
    });
    return subject;
  }.bind(this);
};
MyProgramApi.pollRuns = MyProgramApi.generalGetter('runRecords');
MyProgramApi.runs = MyProgramApi.generalGetter('runRecords');
MyProgramApi.pollStatus = MyProgramApi.generalGetter('status');
module.exports = {MyProgramApi};
