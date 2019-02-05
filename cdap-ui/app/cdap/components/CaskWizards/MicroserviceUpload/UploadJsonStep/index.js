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

import React from 'react';
import { connect, Provider } from 'react-redux';
import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import MicroserviceUploadActions from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';
import FileDnD from 'components/FileDnD';

const mapStateWithDNDFileProps = (state) => {
  return {
    file: state.uploadjson.contents,
    error: state.uploadjson.__error,
  };
};
const mapDispatchWithDNDFileProps = (dispatch) => {
  return {
    onDropHandler: (e) => {
      let reader = new FileReader();
      reader.onload = (evt) => {
        dispatch({
          type: MicroserviceUploadActions.setJson,
          payload: {
            json: evt.target.result,
            jsonFile: e[0],
          },
        });
      };

      reader.readAsText(e[0], 'UTF-8');
    },
  };
};
const MicroserviceJsonUploader = connect(
  mapStateWithDNDFileProps,
  mapDispatchWithDNDFileProps
)(FileDnD);

export default function UploadJsonStep() {
  return (
    <Provider store={MicroserviceUploadStore}>
      <div className="upload-step-container">
        <MicroserviceJsonUploader />
      </div>
    </Provider>
  );
}
