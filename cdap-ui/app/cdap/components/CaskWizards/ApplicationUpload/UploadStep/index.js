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

import React, {PropTypes} from 'react';
import { connect, Provider } from 'react-redux';
import ApplicationUploadStore from 'services/WizardStores/ApplicationUpload/ApplicationUploadStore';
import ApplicationUploadActions from 'services/WizardStores/ApplicationUpload/ApplicationUploadActions';
import Dropzone from 'react-dropzone';
require('./UploadStep.less');

const DragNDropFile = ({file, onDropHandler}) => {
  return (
    <Dropzone
      activeClassName="file-drag-container"
      className="file-drop-container"
      onDrop={onDropHandler}>
      <div className="file-metadata-container text-center">
        {
          file.name && file.name.length ? (<span>{file.name}</span>)
            :
            (<span>
               Drag and Drop the file to be uploaded
              <br />
              or
              <br />
              Click to select file from your computer
            </span>)
        }
      </div>
    </Dropzone>
  );
};

DragNDropFile.propTypes = {
  file: PropTypes.any,
  onDropHandler: PropTypes.func
};

const mapStateToApplicationUploaderProps = (state) => {
  return {
    file: state.uploadFile.file
  };
};
const mapDispatchToApplicationUploadProps = (dispatch) => {
  return {
    onDropHandler: (e) => {
      dispatch({
        type: ApplicationUploadActions.UPLOAD_JAR,
        payload: {
          file: e[0]
        }
      });
    }
  };
};

const ApplicationUploader = connect(
  mapStateToApplicationUploaderProps,
  mapDispatchToApplicationUploadProps
)(DragNDropFile);

export default function ApplicationUploadStep() {
  return (
    <Provider store={ApplicationUploadStore}>
      <div className="application-upload-step">
        <ApplicationUploader />
      </div>
    </Provider>
  );
}
