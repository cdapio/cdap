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

import React, { Component, PropTypes } from 'react';
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
import SchemaStore from 'components/SchemaEditor/SchemaStore';
import SchemaEditor from 'components/SchemaEditor';
import {getParsedSchema} from 'components/SchemaEditor/SchemaHelpers';
import MyDataPrepApi from 'api/dataprep';
import DataPrepStore from 'components/DataPrep/store';
import fileDownload from 'react-file-download';
import NamespaceStore from 'services/NamespaceStore';

export default class SchemaModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: true,
      error: null,
      schema: []
    };

    this.download = this.download.bind(this);
  }

  componentWillUnmount() {
    SchemaStore.dispatch({
      type: 'RESET'
    });
  }

  componentDidMount() {
    let state = DataPrepStore.getState().dataprep;
    let workspaceId = state.workspaceId;

    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestObj = {
      namespace,
      workspaceId
    };

    let directives = state.directives;

    if (directives) {
      requestObj.directive = directives;
    }

    MyDataPrepApi.getSchema(requestObj)
      .subscribe((res) => {
        let tempSchema = {
          name: 'avroSchema',
          type: 'record',
          fields: res
        };

        let fields = getParsedSchema(tempSchema);
        SchemaStore.dispatch({
          type: 'FIELD_UPDATE',
          payload: {
            schema: { fields }
          }
        });

        this.setState({
          loading: false,
          schema: res
        });
      }, (err) => {
        console.log('Error fetching Schema', err);

        this.setState({
          loading: false,
          error: err.message
        });
      });
  }

  download() {
    let workspaceId = DataPrepStore.getState().dataprep.workspaceId;
    let filename = `${workspaceId}-schema.json`;

    let data = JSON.stringify(this.state.schema, null, 4);

    fileDownload(data, filename);
  }

  render() {
    let content;

    if (this.state.loading) {
      content = (
        <div className="text-xs-center">
          <h4>
            <span className="fa fa-spin fa-spinner" />
          </h4>
        </div>
      );
    } else if (this.state.error) {
      content = (
        <div className="text-xs-center text-danger">
          {this.state.error}
        </div>
      );
    } else {
      content = (
        <fieldset disabled>
          <SchemaEditor />
        </fieldset>
      );
    }

    return (
      <Modal
        isOpen={true}
        toggle={this.props.toggle}
        zIndex="1070"
        className="dataprep-schema-modal"
      >
        <ModalHeader>
          <span>
            Schema
          </span>

          <div
            className="close-section float-xs-right"
          >
            <span
              className="fa fa-download"
              onClick={this.download}
            />
            <span
              className="fa fa-times"
              onClick={this.props.toggle}
            />
          </div>
        </ModalHeader>
        <ModalBody>
          {content}
        </ModalBody>
      </Modal>
    );
  }
}

SchemaModal.propTypes = {
  toggle: PropTypes.func
};

