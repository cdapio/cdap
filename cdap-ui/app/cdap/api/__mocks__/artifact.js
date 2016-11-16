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

import Rx from 'rx';

const MyArtifactApi = {
  __artifactDetail: {},
  __extensions: []
};

MyArtifactApi.getIcon = function getIcon() {
  return '/some/random/Image/path';
};

function setArtifactDetail(__artifactDetail) {
  this.__artifactDetail = __artifactDetail;
}

MyArtifactApi.get = function() {
  let subject = new Rx.Subject();
  setTimeout(() => {
    subject.onNext(this.__artifactDetail);
  });
  return subject;
};
MyArtifactApi.listExtensions = function() {
  let subject = new Rx.Subject();
  setTimeout(() => {
    subject.onNext(this.__extensions);
  });
  return subject;
};


MyArtifactApi.__setArtifactDetail = setArtifactDetail.bind(MyArtifactApi);

module.exports = {MyArtifactApi};
