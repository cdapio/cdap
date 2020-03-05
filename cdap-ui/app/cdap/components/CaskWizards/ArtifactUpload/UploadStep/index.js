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

import React from 'react';
import { connect, Provider } from 'react-redux';
import ArtifactUploadStore from 'services/WizardStores/ArtifactUpload/ArtifactUploadStore';
import ArtifactUploadActions from 'services/WizardStores/ArtifactUpload/ArtifactUploadActions';
import { getArtifactNameAndVersion } from 'services/helpers';
import FileDnD from 'components/FileDnD';
require('./UploadStep.scss');

const mapStateWithDNDFileProps = (state) => {
  return {
    file: state.upload.file,
  };
};
const mapDispatchWithDNDFileProps = (dispatch) => {
  return {
    onDropHandler: (e) => {
      let { version } = getArtifactNameAndVersion(e[0].name.split('.jar')[0]);
      dispatch({
        type: ArtifactUploadActions.setVersion,
        payload: { version },
      });
      dispatch({
        type: ArtifactUploadActions.setFilePath,
        payload: {
          file: e[0],
        },
      });
    },
  };
};
const ArtifactUploader = connect(mapStateWithDNDFileProps, mapDispatchWithDNDFileProps)(FileDnD);

export default function UploadStep() {
  return (
    <Provider store={ArtifactUploadStore}>
      <div className="upload-step-container">
        <ArtifactUploader />
      </div>
    </Provider>
  );
}
