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
import {Provider} from 'react-redux';
import CreateStreamStore from 'services/WizardStores/CreateStream/CreateStreamStore';
import Dropzone from 'react-dropzone';

require('./UploadStep.less');

export default function UploadStep() {
  const onFileDrop = (files) => {
    console.log('FILES: ', files);
  };
  return (
    <Provider store={CreateStreamStore}>
      <div className="wizard-upload-step">
        <div className="well well-lg">
          <Dropzone
            onDrop={onFileDrop}
            className="drop-container"
            activeClassName="on-drag-over-container"
          >
            <span className="file-drop-placeholder">
              Select file to be uploaded to Stream
            </span>
          </Dropzone>
        </div>
      </div>
    </Provider>
  );
}
