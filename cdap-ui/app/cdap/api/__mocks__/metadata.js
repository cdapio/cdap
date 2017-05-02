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

const MyMetadataApi = {
  __metadata: {},
  __properties: {},
  __tags: []
};


MyMetadataApi.__setMetadata = function(metadata, isError) {
  this.__isError = isError;
  this.__metadata = metadata;
};

MyMetadataApi.__setProperties = function(properties, isError) {
  this.__isError = isError;
  this.__properties = properties;
};
MyMetadataApi.__setTags = function(tags, isError) {
  this.__isError = isError;
  this.__tags = tags;
};


MyMetadataApi.generalGetter = function(property) {
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
MyMetadataApi.resetState = function() {
  this.__metadata = {};
  this.__properties = {};
  this.__isError = false;
};
MyMetadataApi.getProperties = MyMetadataApi.generalGetter('__properties');
MyMetadataApi.getMetadata = MyMetadataApi.generalGetter('__metadata');
MyMetadataApi.getTags = MyMetadataApi.generalGetter('__tags');

module.exports = {MyMetadataApi};
