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
import React, {Component, PropTypes} from 'react';
import {connect, Provider} from 'react-redux';

import {Form} from 'reactstrap';
require('./UploadData.less');
import Dropzone from 'react-dropzone';
import UploadDataAction from 'services/WizardStores/UploadData/UploadDataActions';
import UploadDataStore from 'services/WizardStores/UploadData/UploadDataStore';

const DragNDropFile = ({filename, onDropHandler}) => {
  return (
    <Form className="form-horizontal wizard-upload-data">
      <Dropzone
        activeClassName="file-drag-container"
        className="file-drop-container"
        onDrop={onDropHandler}>
        <h1 className="file-metadata-container">
          {filename}
        </h1>
      </Dropzone>
    </Form>
  );
};
DragNDropFile.propTypes = {
  filename: PropTypes.string,
  onDropHandler: PropTypes.func
};
const mapStateWithDNDFileProps = (state) => {
  console.log('Default Data:', state);
  return {
    filename: state.viewdata.filename
  };
};
const mapDispatchWithDNDFileProps = (dispatch) => {
  return {
    onDropHandler: (e) => {
      dispatch({
        type: UploadDataAction.setFilename,
        payload: {
          filename: e[0].name
        }
      });
      dispatch({
        type: UploadDataAction.setDefaultData,
        payload: {
          data: e[0]
        }
      });
    }
  };
};
const StreamFileUploader = connect(
  mapStateWithDNDFileProps,
  mapDispatchWithDNDFileProps
)(DragNDropFile);


export default class UploadData extends Component {
  constructor(props) {
    super(props);
    this.state = {
      filename: ''
    };
  }
  onFileDrop(files) {
    console.log(files);
    this.setState({
      filename: files[0].name
    });
  }
  render() {
    return (
      <Provider store={UploadDataStore}>
        <StreamFileUploader />
      </Provider>
    );
  }
}
