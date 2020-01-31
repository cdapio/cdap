/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import UploadFile from 'services/upload-file';
import Cookies from 'universal-cookie';
import NamespaceStore from 'services/NamespaceStore';
import ArtifactUploadStore from 'services/WizardStores/ArtifactUpload/ArtifactUploadStore';
import isNil from 'lodash/isNil';
import { getArtifactNameAndVersion } from 'services/helpers';

const cookie = new Cookies();

// FIXME: Extract it out???
const uploadArtifact = (includeParents = true) => {
  const state = ArtifactUploadStore.getState();

  let filename;
  if (state.upload.file.name && state.upload.file.name.length !== 0) {
    filename = state.upload.file.name.split('.jar')[0];
  }
  let { name } = getArtifactNameAndVersion(filename);
  let namespace = NamespaceStore.getState().selectedNamespace;

  let url = `/namespaces/${namespace}/artifacts/${name}`;

  let headers = {
    'Content-Type': 'application/octet-stream',
    'X-Archive-Name': name,
    'Artifact-Version': state.configure.version,
    'Artifact-Plugins': JSON.stringify([
      {
        name: state.configure.name,
        type: state.configure.type,
        className: state.configure.classname,
        description: state.configure.description,
      },
    ]),
  };

  if (includeParents) {
    headers['Artifact-Extends'] = state.configure.parentArtifact.join('/');
  }

  if (window.CDAP_CONFIG.securityEnabled) {
    let token = cookie.get('CDAP_Auth_Token');
    if (!isNil(token)) {
      headers.Authorization = `Bearer ${token}`;
    }
  }
  return UploadFile({ url, fileContents: state.upload.file, headers });
};
const ArtifactUploadActionCreator = {
  uploadArtifact,
};

export default ArtifactUploadActionCreator;
