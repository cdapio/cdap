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

import ApplicationUploadStore from 'services/WizardStores/ApplicationUpload/ApplicationUploadStore';
import NamespaceStore from 'services/NamespaceStore';
import Cookies from 'universal-cookie';
import UploadFile from 'services/upload-file';
import isNil from 'lodash/isNil';

const cookie = new Cookies();

const UploadApplication = () => {
  let state = ApplicationUploadStore.getState();
  let name = state.uploadFile.file.name;
  let namespace = NamespaceStore.getState().selectedNamespace;
  let url = `/namespaces/${namespace}/apps`;
  let headers = {
    'Content-Type': 'application/octet-stream',
    'X-Archive-Name': name,
  };
  if (window.CDAP_CONFIG.securityEnabled) {
    let token = cookie.get('CDAP_Auth_Token');
    if (!isNil(token)) {
      headers.Authorization = `Bearer ${token}`;
    }
  }
  return UploadFile({ url, fileContents: state.uploadFile.file, headers });
};

export { UploadApplication };
