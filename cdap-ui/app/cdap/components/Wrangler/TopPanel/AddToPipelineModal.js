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
import MyWranglerApi from 'api/wrangler';
import WranglerStore from 'components/Wrangler/store';

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
    let state = WranglerStore.getState().wrangler;
    let workspaceId = state.workspaceId;

    let requestObj = {
      namespace: 'default',
      workspaceId
    };

    let directives = state.directives;
    if (directives) {
      requestObj.directive = directives;
    }

    MyArtifactApi.list({namespace: 'default'})
      .combineLatest(MyWranglerApi.getSchema(requestObj))
      .subscribe((res) => {
        let batchArtifact = find(res[0], { 'name': 'cdap-data-pipeline' });
        let realtimeArtifact = find(res[0], { 'name': 'cdap-data-streams' });
        let wranglerArtifact = find(res[0], { 'name': 'wrangler-transform' });

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
            namespace: 'default',
            configParams: realtimeConfig
          }
        });

        let batchUrl = window.getHydratorUrl({
          stateName: 'hydrator.create',
          stateParams: {
            namespace: 'default',
            configParams: batchConfig
          }
        });

        this.setState({
          loading: false,
          realtimeUrl,
          batchUrl
        });

      }, (err) => {
        console.log('Failed to fetch schema', err);
        this.setState({
          error: err.message,
          loading: false
        });
      });
  }

  render() {
    let content;

    if (this.state.loading) {
      content = (
        <div>
          <h4 className="text-xs-center">
            <span className="fa fa-spin fa-spinner" />
          </h4>
        </div>
      );
    } else if (this.state.error) {
      content = (
        <div>
          <h4 className="text-danger text-xs-center">
            {this.state.error}
          </h4>
        </div>
      );
    } else {
      content = (
        <div className="hydrator-pipeline-content-container">
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
        zIndex="1070"
        className="add-to-hydrator-wrangler-modal"
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

