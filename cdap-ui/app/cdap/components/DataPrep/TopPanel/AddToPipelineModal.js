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
import {MyArtifactApi} from 'api/artifact';
import find from 'lodash/find';
import MyDataPrepApi from 'api/dataprep';
import DataPrepStore from 'components/DataPrep/store';
import NamespaceStore from 'services/NamespaceStore';
import {findHighestVersion} from 'services/VersionRange/VersionUtilities';
import {objectQuery} from 'services/helpers';
import T from 'i18n-react';
import {getParsedSchemaForDataPrep} from 'components/SchemaEditor/SchemaHelpers';
import {directiveRequestBodyCreator} from 'components/DataPrep/helper';

const mapErrorToMessage = (e) => {
  let message = e.message;
  if (message.indexOf('invalid field name') !== -1) {
    let splitMessage = e.message.split("field name: ");
    let fieldName = objectQuery(splitMessage, 1) || e.message;
    return {
      message: T.translate('features.DataPrep.TopPanel.invalidFieldNameMessage', {fieldName}),
      remedies: `
${T.translate('features.DataPrep.TopPanel.invalidFieldNameRemedies1')}
${T.translate('features.DataPrep.TopPanel.invalidFieldNameRemedies2')}
      `
    };
  }
  return {message: e.message};
};

export default class AddToHydratorModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: true,
      batchUrl: null,
      realtimeUrl: null,
      error: null
    };
  }

  componentWillMount() {
    this.generateLinks();
    window.onbeforeunload = null;
  }

  componentWillUnmount() {
    window.onbeforeunload = function() {
      return "Are you sure you want to leave this page?";
    };
  }

  generateLinks() {
    let namespace = NamespaceStore.getState().selectedNamespace;

    MyDataPrepApi.getInfo({ namespace })
      .subscribe((res) => {
        let pluginVersion = res.values[0]['plugin.version'];

        this.constructProperties(pluginVersion);
      }, (err) => {
        if (err.statusCode === 404) {
          console.log('cannot find method');
          // can't find method; use latest wrangler-transform
          this.constructProperties();
        }
      });
  }

  findWranglerArtifacts(artifacts, pluginVersion) {
    let wranglerArtifacts = artifacts.filter((artifact) => {
      if (pluginVersion) {
        return artifact.name === 'wrangler-transform' && artifact.version === pluginVersion;
      }

      return artifact.name === 'wrangler-transform';
    });

    if (wranglerArtifacts.length === 0) {
      // cannot find plugin. Error out
      this.setState({
        error: 'Cannot find wrangler-transform plugin. Please load wrangler transform from Cask Market'
      });

      return null;
    }

    let filteredArtifacts = wranglerArtifacts;

    if (!pluginVersion) {
      let highestVersion = findHighestVersion(wranglerArtifacts.map((artifact) => {
        return artifact.version;
      }), true);

      filteredArtifacts = wranglerArtifacts.filter((artifact) => {
        return artifact.version === highestVersion;
      });
    }

    let returnArtifact = filteredArtifacts[0];

    if (filteredArtifacts.length > 1) {
      returnArtifact.scope = 'USER';
    }

    return returnArtifact;
  }

  constructProperties(pluginVersion) {
    let namespace = NamespaceStore.getState().selectedNamespace;
    let state = DataPrepStore.getState().dataprep;
    let workspaceId = state.workspaceId;

    let requestObj = {
      namespace,
      workspaceId
    };

    let directives = state.directives;

    let requestBody = directiveRequestBodyCreator(directives);

    MyArtifactApi.list({ namespace })
      .combineLatest(MyDataPrepApi.getSchema(requestObj, requestBody))
      .subscribe((res) => {
        let batchArtifact = find(res[0], { 'name': 'cdap-data-pipeline' });
        let realtimeArtifact = find(res[0], { 'name': 'cdap-data-streams' });
        let wranglerArtifact = this.findWranglerArtifacts(res[0], pluginVersion);

        let tempSchema = {
          name: 'avroSchema',
          type: 'record',
          fields: res[1]
        };

        let properties = {
          field: '*',
          directives: directives.join('\n'),
          schema: JSON.stringify(tempSchema)
        };

        try {
          getParsedSchemaForDataPrep(tempSchema);
        } catch (e) {
          let {message, remedies = null} = mapErrorToMessage(e);
          this.setState({
            error: {message, remedies},
            loading: false
          });
          return;
        }

        // Generate hydrator config as URL parameters
        let config = {
          config: {
            source: {},
            transforms: [{
              name: 'Wrangler',
              plugin: {
                name: 'Wrangler',
                label: 'Wrangler',
                artifact: wranglerArtifact,
                properties
              }
            }],
            sinks:[],
            connections: []
          }
        };

        let realtimeConfig = Object.assign({}, config, {artifact: realtimeArtifact});
        let batchConfig = Object.assign({}, config, {artifact: batchArtifact});

        let realtimeUrl = window.getHydratorUrl({
          stateName: 'hydrator.create',
          stateParams: {
            namespace,
            configParams: realtimeConfig
          }
        });

        let batchUrl = window.getHydratorUrl({
          stateName: 'hydrator.create',
          stateParams: {
            namespace,
            configParams: batchConfig
          }
        });

        this.setState({
          loading: false,
          realtimeUrl,
          batchUrl
        });

      }, (err) => {
        this.setState({
          error: objectQuery(err, 'response', 'message')  || T.translate('features.DataPrep.TopPanel.PipelineModal.defaultErrorMessage'),
          loading: false
        });
      });
  }

  render() {
    let content;

    if (this.state.loading) {
      content = (
        <div className="loading-container">
          <h4 className="text-xs-center">
            <span className="fa fa-spin fa-spinner" />
          </h4>
        </div>
      );
    } else if (this.state.error) {
      content = (
        <div>
          <div className="text-danger error-message-container loading-container">
            <span className="fa fa-exclamation-triangle"></span>
            <span>
              {typeof this.state.error === 'object' ? this.state.error.message : this.state.error}
            </span>
            <pre>
              {
                objectQuery(this.state, 'error', 'remedies') ? this.state.error.remedies : null
              }
            </pre>
          </div>
        </div>
      );
    } else {
      content = (
        <div>
          <div className="message">
            Choose the type of pipeline to create
          </div>
          <div className="action-buttons">
            <a
              href={this.state.batchUrl}
              className="btn btn-secondary"
            >
              <i className="fa icon-ETLBatch"/>
              <span>Batch Pipeline</span>
            </a>
            <a
              href={this.state.realtimeUrl}
              className="btn btn-secondary"
            >
              <i className="fa icon-sparkstreaming"/>
              <span>Realtime Pipeline</span>
            </a>
          </div>
        </div>
      );
    }

    return (
      <Modal
        isOpen={true}
        toggle={this.props.toggle}
        size="lg"
        className="add-to-pipeline-dataprep-modal"
      >
        <ModalHeader>
          <span>
            Add to Pipeline
          </span>

          <div
            className="close-section float-xs-right"
            onClick={this.props.toggle}
          >
            <span className="fa fa-times" />
          </div>
        </ModalHeader>
        <ModalBody>
          {content}
        </ModalBody>
      </Modal>
    );
  }
}

AddToHydratorModal.propTypes = {
  toggle: PropTypes.func
};
