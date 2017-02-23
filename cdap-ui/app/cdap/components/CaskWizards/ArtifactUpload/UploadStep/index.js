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

import React, { PropTypes } from 'react';
import { connect, Provider } from 'react-redux';
import ArtifactUploadStore from 'services/WizardStores/ArtifactUpload/ArtifactUploadStore';
import ArtifactUploadActions from 'services/WizardStores/ArtifactUpload/ArtifactUploadActions';
import T from 'i18n-react';
import FileDnD from 'components/FileDnD';
require('./UploadStep.scss');

const mapStateWithDNDFileProps = (state) => {
  return {
    file: state.upload.file
  };
};
const mapDispatchWithDNDFileProps = (dispatch) => {
  return {
    onDropHandler: (e) => {
      dispatch({
        type: ArtifactUploadActions.setFilePath,
        payload: {
          file: e[0]
        }
      });
    }
  };
};
const ArtifactUploader = connect(
  mapStateWithDNDFileProps,
  mapDispatchWithDNDFileProps
)(FileDnD);


export default function UploadStep(undefined, context) {
  return (
    <Provider store={ArtifactUploadStore}>
      <div className="upload-step-container">
        {
          /* TODO: shouldn't do this, replace in 4.2} */
          context.isMarket ?
            (
              <h4 className="upload-instruction">
                {T.translate('features.Wizard.ArtifactUpload.Step1.uploadHelperText')}
              </h4>
            )
          :
            null
        }
        <ArtifactUploader />
      </div>
    </Provider>
  );
}
UploadStep.contextTypes = {
  isMarket: PropTypes.bool
};
