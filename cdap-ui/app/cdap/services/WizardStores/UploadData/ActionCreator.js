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

import {MyMarketApi} from 'api/market';
import UploadDataAction from 'services/WizardStores/UploadData/UploadDataActions';
import UploadDataStore from 'services/WizardStores/UploadData/UploadDataStore';
import 'whatwg-fetch';
import Rx from 'rx';
import isNil from 'lodash/isNil';

const fetchDefaultData = ({filename, packagename: entityName, packageversion: entityVersion}) => {
  UploadDataStore.dispatch({
    type: UploadDataAction.setDefaultDataLoading,
    payload: {}
  });
  MyMarketApi
    .getSampleData({ entityName, entityVersion, filename })
    .subscribe((data) => {
      UploadDataStore.dispatch({
        type: UploadDataAction.setDefaultData,
        payload: { data }
      });
    });
};
// FIXME: Extract it out???
const uploadData = ({url, fileContents, headers}) => {
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
  if (!isNil(headers.authToken)) {
    xhr.setRequestHeader('Authorization', 'Bearer ' + headers.authToken);
  }
  xhr.send(fileContents);
  xhr.onreadystatechange = function () {
    if (xhr.readyState === 4) {
      if (xhr.status > 200) {
        subject.onError(xhr.response);
      } else {
        subject.onNext(true);
      }
    }
  };
  return subject;
};
const UploadDataActionCreator = {
  fetchDefaultData,
  uploadData
};

export default UploadDataActionCreator;
