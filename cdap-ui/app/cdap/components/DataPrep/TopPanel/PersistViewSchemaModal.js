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
import { Modal, ModalHeader, ModalBody, ModalFooter, Button } from 'reactstrap';
import DataPrepStore from 'components/DataPrep/store';
import { objectQuery } from 'services/helpers';
import T from 'i18n-react';
import getPipelineConfig from 'components/DataPrep/TopPanel/PipelineConfigHelper';
import { getSchemaObjFromFieldsArray } from 'components/SchemaEditor/SchemaHelpers';
import MyDataPrepApi from 'api/dataprep';
import MyFeatureEngineeringApi from 'api/featureengineeringapp';
import NamespaceStore from 'services/NamespaceStore';
import { directiveRequestBodyCreator, viewSchemaPersistRequestBodyCreator } from 'components/DataPrep/helper';

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


export default class PersistViewSchemaModel extends Component {
  constructor(props) {
    super(props);

    this.state = {
      configloading: true,
      schemaloading: true,
      loading: true,
      error: null,
      workspaceId: null,
      realtimeConfig: null,
      batchConfig: null,
      schema: [],
      response: null,
      datasetName: "",
      formloaded: false,
      navigateFE: false,
    };
  }

  componentDidMount() {
    this.generateLinks();
    this.getSchema();
  }

  persistViewSchema() {
    // if (!this.state.loading) {
    //   return;
    // }
    this.setState({
      error: false,
      loading: true,
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
    MyFeatureEngineeringApi
      .persistWranglerPluginConfig(requestObj, requestBody)
      .subscribe(
        (res) => {
          this.setState({
            navigateFE: true,
            loading: false,
            response: objectQuery(res, 'response', 'message') || JSON.stringify(res)
          });
        },
        (err) => {
          this.setState({
            loading: false,
            error: objectQuery(err, 'response', 'message') || JSON.stringify(err)
          });
          console.log('Error', err);
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
    this.setState({ datasetName: e.target.value });
  }


  handleSubmit = () => {
    this.setState({ formloaded: true });
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
          error: objectQuery(err, 'response', 'message') || T.translate('features.DataPrep.TopPanel.SchemaModal.defaultErrorMessage')
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
    let inputStyle = {
      'width': '300px',
      'margin-left': '10px'
    };
    let modalStyle = {
      'width': '500px'
    };
    if (!this.state.configloading && !this.state.schemaloading && !this.state.formloaded) {
      content = null;
    } else {
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

          </div>
        );
      } else {
        content = (
          <div className="remedy-message">
            {this.state.response}
          </div>
        );
      }
    }

    return (
      <Modal style={modalStyle}
        isOpen={true}
        toggle={this.props.toggle}
        size="lg"
        zIndex="1061"
        className="dataprep-schema-modal"
      >
        <ModalHeader>Persist Dataset</ModalHeader>
        <ModalBody>
            <div className="text-xs-left">
              <label>
                Dataset Name:
                  <input type="text" style={inputStyle} value={this.state.datasetName} onChange={this.handleChange} />
              </label>
            </div>

          {content}
        </ModalBody>
        <ModalFooter>
          {
            this.state.navigateFE ?
            <Button className="btn-margin" color="primary" onClick={this.navigateToFeature}>Continue in FeatureEngineering</Button>
            :null
          }

          <Button className="btn-margin" color="secondary" onClick={this.props.toggle}>Cancel</Button>
          <Button className="btn-margin" color="primary" onClick={this.handleSubmit}
            disabled={this.state.datasetName.trim().length < 1} >OK</Button>
        </ModalFooter>
      </Modal>
    );
  }
}

PersistViewSchemaModel.propTypes = {
  toggle: PropTypes.func
};
