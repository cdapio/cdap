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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import { Modal, ModalHeader, ModalBody, ModalFooter, Button, FormGroup, Label, Col} from 'reactstrap';
import DataPrepStore from 'components/DataPrep/store';
import { objectQuery } from 'services/helpers';
import T from 'i18n-react';
import getPipelineConfig from 'components/DataPrep/TopPanel/PipelineConfigHelper';
import { getSchemaObjFromFieldsArray } from 'components/SchemaEditor/SchemaHelpers';
import MyDataPrepApi from 'api/dataprep';
import MyFeatureEngineeringApi from 'api/featureengineeringapp';
import NamespaceStore from 'services/NamespaceStore';
import { directiveRequestBodyCreator, viewSchemaPersistRequestBodyCreator } from 'components/DataPrep/helper';
import isNil from 'lodash/isNil';
import cookie from 'react-cookie';
import types from 'services/inputValidationTemplates';
import ValidatedInput from 'components/ValidatedInput';
const PREFIX = 'features.DataPrep.TopPanel';
const mapErrorToMessage = (message) => {
  if (message.indexOf('invalid field name') !== -1) {
    let splitMessage = message.split("field name: ");
    let fieldName = objectQuery(splitMessage, 1) || message;
    return {
      message: T.translate(`${PREFIX}.invalidFieldNameMessage`, { fieldName }),
      remedies: `${T.translate(`${PREFIX}.invalidFieldNameRemedies1`)}`
    };
  }
  return { message };
};


export default class PersistViewSchemaModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      configloading: true,
      schemaloading: true,
      loading: false,
      error: null,
      workspaceId: null,
      realtimeConfig: null,
      batchConfig: null,
      schema: [],
      response: null,
      datasetName: "",
      formloaded: false,
      navigateFE: false,
      inputError: '',
      inputTemplate: 'NAME'
    };
  }

  componentDidMount() {
    this.generateLinks();
    this.getSchema();
  }

  persistViewSchema() {
    this.setState({
      error: false,
      loading: true,
      inputError: ''
    });

    let config = this.state.realtimeConfig;
    let configType = 'realTime';
    if (!this.state.batchUrl) {
      configType = 'batch';
      config = this.state.batchConfig;
    }
    let datasetName = this.state.datasetName;
    let namespace = NamespaceStore.getState().selectedNamespace;
    let requestObj = {
      namespace: namespace,
      datasetName: datasetName,
      configType: configType
    };
    let requestBody = viewSchemaPersistRequestBodyCreator(JSON.stringify([getSchemaObjFromFieldsArray(this.state.schema)], null, 4), JSON.stringify(config));
    let requestHeaders = {};
    if (!isNil(cookie.load('CDAP_Auth_Token'))) {
      requestHeaders["AccessToken"] = `Bearer ${cookie.load('CDAP_Auth_Token')}`;
    }

    MyFeatureEngineeringApi
      .persistWranglerPluginConfig(requestObj, requestBody, requestHeaders)
      .subscribe(
        (res) => {
          this.setState({
            navigateFE: true,
            loading: false,
            response: objectQuery(res, 'message') || JSON.stringify(res)
          });
        },
        (err) => {
          this.setState({
            loading: false,
            error: objectQuery(err, 'message') || JSON.stringify(err)
          });
        }
      );
  }

  generateLinks() {
    let state = DataPrepStore.getState().dataprep;
    let workspaceId = state.workspaceId;

    getPipelineConfig().subscribe(
      (res) => {

        this.setState({
          worspaceId: workspaceId,
          realtimeConfig: res.realtimeConfig,
          batchConfig: res.batchConfig,
          configloading: false
        });

      },
      (err) => {
        let { message, remedies = null } = mapErrorToMessage(err);

        if (remedies) {
          this.setState({
            error: { message, remedies },
            configloading: false
          });
          return;
        }

        this.setState({
          error: err,
          configloading: false
        });
      }
    );
  }

  handleChange = (e) => {
    const isValid = types[this.state.inputTemplate].validate(e.target.value);
    let errorMsg = '';
    if (e.target.value && !isValid) {
      errorMsg = 'Invalid Input, see help.';
    }
    if (!e.target.value) {
      errorMsg = 'Dataset Name is required.';
    }
    this.setState({
      datasetName: e.target.value,
      inputError: errorMsg
    });
  }


  handleSubmit = () => {
    this.setState({ formloaded: true,error: false,loading: true, });
    this.persistViewSchema();
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

        this.setState({
          schema: res,
          schemaloading: false
        });
      }, (err) => {
        this.setState({
          schemaloading: false,
          error: objectQuery(err, 'message') || T.translate('features.DataPrep.TopPanel.SchemaModal.defaultErrorMessage')
        });
      });
  }

   navigateToFeature = () => {
    const namespace = NamespaceStore.getState().selectedNamespace;
    const feURL = `/ns/${namespace}/featureEngineering`;
    const fePath = `/cdap${feURL}`;
    window.location.href = fePath;
  }

  render() {
    let content;

    if (!this.state.configloading && !this.state.schemaloading && !this.state.formloaded) {
      content = null;
    } else {
      if (this.state.loading) {
        content = (
          <div className="text-xs-center text-wrap">
            <h4>
              <span className="fa fa-spin fa-spinner" />
            </h4>
          </div>
        );
      } else if (this.state.error) {
        content = (
          <div>
            <div className="text-danger text-wrap">
              <span className="fa fa-exclamation-triangle"></span>
              <span>
                {typeof this.state.error === 'object' ? this.state.error.message : this.state.error}
              </span>
            </div>
            <div className="remedy-message text-wrap">
              {
                objectQuery(this.state, 'error', 'remedies') ? this.state.error.remedies : null
              }
            </div>

          </div>
        );
      } else {
        content = (
          <div className="success-message text-wrap" title={this.state.response}>
            {this.state.response}
          </div>
        );
      }
    }

    return (
      <Modal
        isOpen={true}
        toggle={this.props.toggle}
        size="lg"
        zIndex="1061"
        className="persist-view-schema-modal"
      >
        <ModalHeader>Persist Dataset</ModalHeader>
        <ModalBody>
          <FormGroup row>
            <Label xs="4" className="text-xs-left">
              Dataset Name:
              <span className="text-danger">*</span>
            </Label>
            <Col xs="7" className="dataset-name-group">
              <ValidatedInput
                type="text"
                label="Dataset Name"
                inputInfo={types[this.state.inputTemplate].getInfo()}
                validationError={this.state.inputError}
                value={this.state.datasetName}
                onChange={this.handleChange}
              />
            </Col>
          </FormGroup>
        </ModalBody>
        <ModalFooter>
          {content}
          <fieldset className='buttons-container' disabled={this.state.loading}>
            {
              this.state.navigateFE ?
              <div>
                <Button className="btn-margin" color="primary" onClick={this.navigateToFeature}>Continue in FeatureEngineering</Button>
                <Button className="btn-margin" color="secondary" onClick={this.props.toggle}>Close</Button>
              </div>
              :
              <div>
                <Button className="btn-margin" color="secondary" onClick={this.props.toggle}>Close</Button>
                <Button className="btn-margin" color="primary" onClick={this.handleSubmit}
                  disabled={this.state.datasetName.trim().length < 1 || this.state.inputError.length > 0} >OK</Button>
            </div>
            }
          </fieldset>

        </ModalFooter>
      </Modal>
    );
  }
}

PersistViewSchemaModal.propTypes = {
  toggle: PropTypes.func
};
