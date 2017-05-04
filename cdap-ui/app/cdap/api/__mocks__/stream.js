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
import Rx from 'rx';
const MyStreamApi = {
  __list: [],
  __programs: [],
  isError: false
};

MyStreamApi.generalGetter = function(property) {
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

MyStreamApi.list = MyStreamApi.generalGetter('__list');
MyStreamApi.__setList = function(list, isError) {
  this.__isError = isError;
  this.__list = list;
};
MyStreamApi.getPrograms = MyStreamApi.generalGetter('__programs');
MyStreamApi.__setPrograms = function(programs, isError) {
  this.__isError = isError;
  this.__programs = programs;
};
export {MyStreamApi};
