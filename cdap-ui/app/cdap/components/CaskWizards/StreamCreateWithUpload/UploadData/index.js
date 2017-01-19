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
import React, {Component} from 'react';
import {connect, Provider} from 'react-redux';
import FileDnD from 'components/FileDnD';
import CreateStreamWithUploadActions from 'services/WizardStores/CreateStreamWithUpload/CreateStreamWithUploadActions';
import CreateStreamWithUploadStore from 'services/WizardStores/CreateStreamWithUpload/CreateStreamWithUploadStore';
require('./UploadData.scss');

const mapStateWithDNDFileProps = (state) => {
  return {
    file: state.upload.data
  };
};
const mapDispatchWithDNDFileProps = (dispatch) => {
  return {
    onDropHandler: (e) => {
      dispatch({
        type: CreateStreamWithUploadActions.setData,
        payload: {
          filename: e[0].name,
          data: e[0]
        }
      });
    }
  };
};
const StreamFileUploader = connect(
  mapStateWithDNDFileProps,
  mapDispatchWithDNDFileProps
)(FileDnD);


export default class UploadData extends Component {
  constructor(props) {
    super(props);
    this.state = {
      filename: ''
    };
  }
  onFileDrop(files) {
    this.setState({
      filename: files[0].name
    });
  }
  render() {
    return (
      <Provider store={CreateStreamWithUploadStore}>
        <div className="upload-step-container">
          <StreamFileUploader />
        </div>
      </Provider>
    );
  }
}
