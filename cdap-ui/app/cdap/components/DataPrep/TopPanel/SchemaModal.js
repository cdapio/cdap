/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import { Modal, ModalHeader, ModalBody } from 'reactstrap';
import MyDataPrepApi from 'api/dataprep';
import DataPrepStore from 'components/DataPrep/store';
import fileDownload from 'js-file-download';
import NamespaceStore from 'services/NamespaceStore';
import { objectQuery, isNilOrEmpty } from 'services/helpers';
import T from 'i18n-react';
import { directiveRequestBodyCreator } from 'components/DataPrep/helper';
import { execute } from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import CardActionFeedback from 'components/CardActionFeedback';
import If from 'components/If';
import cdapavsc from 'services/cdapavscwrapper';
import { SchemaEditor } from 'components/AbstractWidget/SchemaEditor';

const mapErrorToMessage = (e) => {
  let message = e.message;
  if (message.indexOf('invalid field name') !== -1) {
    let splitMessage = e.message.split('field name: ');
    let fieldName = objectQuery(splitMessage, 1) || e.message;
    return {
      message: T.translate('features.DataPrep.TopPanel.invalidFieldNameMessage', { fieldName }),
      remedies: `${T.translate('features.DataPrep.TopPanel.invalidFieldNameRemedies1')}`,
    };
  }
  return { message: e.message };
};

export default class SchemaModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: true,
      error: null,
      schema: [],
    };

    this.download = this.download.bind(this);
  }

  componentDidMount() {
    this.getSchema();
  }

  getSchema() {
    let state = DataPrepStore.getState().dataprep;
    let workspaceId = state.workspaceId;

    let namespace = NamespaceStore.getState().selectedNamespace;

    let requestObj = {
      context: namespace,
      workspaceId,
    };

    let directives = state.directives;
    let requestBody = directiveRequestBodyCreator(directives);

    MyDataPrepApi.getSchema(requestObj, requestBody).subscribe(
      (res) => {
        const schema = {
          name: 'etlSchemaBody',
          type: 'record',
          fields: res,
        };
        const workspaceSchema = { name: 'etlSchemaBody', schema };

        try {
          cdapavsc.parse(schema);
        } catch (e) {
          const { remedies, error } = mapErrorToMessage(e);
          return this.setState({
            loading: false,
            error,
            remedies,
          });
        }
        this.setState({
          loading: false,
          schema: workspaceSchema,
        });
      },
      (err) => {
        this.setState({
          loading: false,
          error:
            objectQuery(err, 'response', 'message') ||
            T.translate('features.DataPrep.TopPanel.SchemaModal.defaultErrorMessage'),
        });
      }
    );
  }

  applyDirective(directive) {
    execute([directive]).subscribe(
      () => {
        this.setState({
          error: null,
          loading: true,
          schema: [],
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
            message: err.message || err.response.message,
          },
        });
      }
    );
  }

  download() {
    let workspaceId = DataPrepStore.getState().dataprep.workspaceId;
    let filename = `${workspaceId}-schema.json`;
    fileDownload(JSON.stringify([this.state.schema], null, 4), filename);
  }

  render() {
    let content;

    if (this.state.loading) {
      content = (
        <div className="text-center">
          <h4>
            <span className="fa fa-spin fa-spinner" />
          </h4>
        </div>
      );
    } else if (this.state.error) {
      content = (
        <div>
          <div className="remedy-message">
            {objectQuery(this.state, 'error', 'remedies') ? this.state.error.remedies : null}
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
          <SchemaEditor visibleRows={10} schema={this.state.schema} disabled={true} />
        </fieldset>
      );
    }

    return (
      <Modal
        isOpen={true}
        toggle={this.props.toggle}
        size="lg"
        zIndex="1061"
        className="dataprep-schema-modal cdap-modal"
      >
        <ModalHeader>
          <span>Schema</span>

          <div className="close-section float-right">
            <button
              disabled={this.state.error ? 'disabled' : null}
              className="btn btn-link"
              onClick={this.download}
            >
              <span className="fa fa-download" />
            </button>
            <span className="fa fa-times" onClick={this.props.toggle} />
          </div>
        </ModalHeader>
        <ModalBody>{content}</ModalBody>
        <If condition={this.state.error}>
          <CardActionFeedback
            type="DANGER"
            message={!isNilOrEmpty(this.state.error) ? this.state.error.message : this.state.error}
            extendedMessage={this.state.remedies}
          />
        </If>
      </Modal>
    );
  }
}

SchemaModal.propTypes = {
  toggle: PropTypes.func,
};
