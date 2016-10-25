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

// FIXME: Extract it out???
const uploadArtifact = ({url, fileContents, headers}) => {
  let subject = new Rx.Subject();
  let xhr = new window.XMLHttpRequest();
  let path;
  xhr.upload.addEventListener('progress', function (e) {
    if (e.type === 'progress') {
      console.info('App Upload in progress');
    }
  });
  path = url;
  xhr.open('POST', path, true);
  xhr.setRequestHeader('Content-type', headers.filetype);
  xhr.setRequestHeader('X-Archive-Name', headers.filename);
  if (headers.Authorization) {
    xhr.setRequestHeader('Authorization', 'Bearer ' + headers.token);
  }
  xhr.setRequestHeader('Artifact-Version', headers['Artifact-Version']);
  xhr.setRequestHeader('Artifact-Extends', headers['Artifact-Extends']);
  xhr.setRequestHeader('Artifact-Plugins', JSON.stringify(headers['Artifact-Plugins']));

  xhr.send(fileContents);
  xhr.onreadystatechange = function () {
    if (xhr.readyState === 4) {
      if (xhr.status > 399){
        subject.onError(xhr.response);
      } else {
        subject.onNext(true);
      }
    }
  };
  return subject;
};
const ArtifactUploadActionCreator = {
  uploadArtifact
};

export default ArtifactUploadActionCreator;
