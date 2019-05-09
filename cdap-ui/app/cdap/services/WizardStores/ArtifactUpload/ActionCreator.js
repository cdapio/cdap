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
import cookie from 'react-cookie';
import NamespaceStore from 'services/NamespaceStore';
import ArtifactUploadStore from 'services/WizardStores/ArtifactUpload/ArtifactUploadStore';
import isNil from 'lodash/isNil';
import { Observable } from 'rxjs/Observable';

// FIXME: Extract it out???
const uploadArtifact = (includeParents = true) => {
  const state = ArtifactUploadStore.getState();

  let getArtifactNameAndVersion = (nameWithVersion) => {
    if (!nameWithVersion) {
      return {
        version: null,
        name: null,
      };
    }

    // core-plugins-3.4.0-SNAPSHOT.jar
    // extracts version from the jar file name. We then get the name of the artifact (that is from the beginning up to version beginning)
    let regExpRule = new RegExp('(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?(?:[.\\-](.*))?$');
    let version = regExpRule.exec(nameWithVersion);
    if (version && Array.isArray(version)) {
      version = version[0];
    }
    let name = version ? nameWithVersion.substr(0, nameWithVersion.indexOf(version) - 1) : null;
    return { version, name };
  };

  let filename;
  if (state.upload.file.name && state.upload.file.name.length !== 0) {
    filename = state.upload.file.name.split('.jar')[0];
  }
  let { name, version } = getArtifactNameAndVersion(filename);
  if (!name || !version) {
    return Observable.throw('Invalid driver JAR file name. The driver JAR file name must conform to the format <name>-<version>.jar (eg: mysql-connector-java-5.1.39-bin.jar)');
  }
  let namespace = NamespaceStore.getState().selectedNamespace;

  let url = `/namespaces/${namespace}/artifacts/${name}`;

  let headers = {
    'Content-Type': 'application/octet-stream',
    'X-Archive-Name': name,
    'Artifact-Version': version,
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
    let token = cookie.load('CDAP_Auth_Token');
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
