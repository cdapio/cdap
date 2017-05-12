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
import {getParsedSchemaForDataPrep} from 'components/SchemaEditor/SchemaHelpers';
import MyDataPrepApi from 'api/dataprep';
import DataPrepStore from 'components/DataPrep/store';
import fileDownload from 'react-file-download';
import NamespaceStore from 'services/NamespaceStore';
import {objectQuery} from 'services/helpers';
import T from 'i18n-react';
import {directiveRequestBodyCreator} from 'components/DataPrep/helper';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';

const mapErrorToMessage = (e) => {
  let message = e.message;
  if (message.indexOf('invalid field name') !== -1) {
    let splitMessage = e.message.split("field name: ");
    let fieldName = objectQuery(splitMessage, 1) || e.message;
    return {
      message: T.translate('features.DataPrep.TopPanel.invalidFieldNameMessage', {fieldName}),
      remedies: `${T.translate('features.DataPrep.TopPanel.invalidFieldNameRemedies1')}`
    };
  }
  return {message: e.message};
};

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
    this.getSchema();
  }

  getSchema() {
    let state = DataPrepStore.getState().dataprep;
    let workspaceId = state.workspaceId;

    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestObj = {
      namespace,
      workspaceId
    };

    let directives = state.directives;
    let requestBody = directiveRequestBodyCreator(directives);

    MyDataPrepApi.getSchema(requestObj, requestBody)
      .subscribe((res) => {
        let tempSchema = {
          name: 'avroSchema',
          type: 'record',
          fields: res
        };

        let fields;
        try {
          fields = getParsedSchemaForDataPrep(tempSchema);
        } catch (e) {
          let {message, remedies = null} = mapErrorToMessage(e);
          this.setState({
            error: {message, remedies},
            loading: false
          });
        }
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
        this.setState({
          loading: false,
          error: objectQuery(err, 'response', 'message') || T.translate('features.DataPrep.TopPanel.SchemaModal.defaultErrorMessage')
        });
      });
  }

  applyDirective(directive) {
    execute([directive])
      .subscribe(
        () => {
          this.setState({
            error: null,
            loading: true,
            schema: []
          });
          setTimeout(() => {
            this.getSchema();
          });
        },
        (err) => {
          console.log('Error', err);

          DataPrepStore.dispatch({
            type: DataPrepActions.setError,
            payload: {
              message: err.message || err.response.message
            }
          });
        }
      );
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
        <div>
          <div className="text-danger">
            <span className="fa fa-exclamation-triangle"></span>
            <span>
              {typeof this.state.error === 'object' ? this.state.error.message : this.state.error}
            </span>
          </div>
          <div className="remedy-message">
              {
                objectQuery(this.state, 'error', 'remedies') ? this.state.error.remedies : null
              }
            </div>
          <span>
            {T.translate('features.DataPrep.TopPanel.invalidFieldNameRemedies2')}
            <span
              className="btn-link"
              onClick={this.applyDirective.bind(this, 'cleanse-column-names')}
            >
              {T.translate('features.DataPrep.TopPanel.cleanseLinkLabel')}
            </span>
            {T.translate('features.DataPrep.TopPanel.invalidFieldNameRemedies3')}
          </span>
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
        size="lg"
        zIndex="1061"
        className="dataprep-schema-modal"
      >
        <ModalHeader>
          <span>
            Schema
          </span>

          <div
            className="close-section float-xs-right"
          >
            <button
              disabled={this.state.error ? 'disabled' : null}
              className="btn btn-link"
              onClick={this.download}
            >
              <span className="fa fa-download" />
            </button>
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
