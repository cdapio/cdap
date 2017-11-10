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
import { Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import {MyArtifactApi} from 'api/artifact';
import find from 'lodash/find';
import MyDataPrepApi from 'api/dataprep';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import NamespaceStore from 'services/NamespaceStore';
import {findHighestVersion} from 'services/VersionRange/VersionUtilities';
import {objectQuery} from 'services/helpers';
import T from 'i18n-react';
import {getParsedSchemaForDataPrep} from 'components/SchemaEditor/SchemaHelpers';
import {directiveRequestBodyCreator} from 'components/DataPrep/helper';
import classnames from 'classnames';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import CardActionFeedback from 'components/CardActionFeedback';

const mapErrorToMessage = (e) => {
  let message = e.message;
  if (message.indexOf('invalid field name') !== -1) {
    let splitMessage = e.message.split("field name: ");
    let fieldName = objectQuery(splitMessage, 1) || e.message;
    return {
      message: T.translate(`${PREFIX}.invalidFieldNameMessage`, {fieldName}),
      remedies: `${T.translate(`${PREFIX}.invalidFieldNameRemedies1`)}`
    };
  }
  return {message: e.message};
};

const PREFIX = 'features.DataPrep.TopPanel';
export default class AddToHydratorModal extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: true,
      batchUrl: null,
      realtimeUrl: null,
      error: null,
      workspaceId: null,
      realtimeConfig: null,
      batchConfig: null
    };
  }

  componentWillMount() {
    this.generateLinks();
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

  constructFileSource(artifactsList, properties) {
    if (!properties) { return null; }

    let plugin = objectQuery(properties, 'values', '0');

    let pluginName = Object.keys(plugin)[0];

    plugin = plugin[pluginName];
    let batchArtifact = find(artifactsList, { 'name': 'core-plugins' });
    let realtimeArtifact = find(artifactsList, { 'name': 'spark-plugins' });

    if (!batchArtifact) {
      return T.translate(`${PREFIX}.filePipeline.pipelineErrorBatch`);
    }

    if (!realtimeArtifact) {
      return T.translate(`${PREFIX}.filePipeline.pipelineErrorRealtime`);
    }

    batchArtifact.version = '[1.7.0, 3.0.0)';
    realtimeArtifact.version = '[1.7.0, 3.0.0)';

    let batchPluginInfo = {
      name: plugin.name,
      label: plugin.name,
      type: 'batchsource',
      artifact: batchArtifact,
      properties: plugin.properties
    };

    let realtimePluginInfo = Object.assign({}, batchPluginInfo, {
      type: 'streamingsource',
      artifact: realtimeArtifact
    });

    let batchStage = {
      name: 'File',
      plugin: batchPluginInfo
    };

    let realtimeStage = {
      name: 'File',
      plugin: realtimePluginInfo
    };

    return {
      batchSource: batchStage,
      realtimeSource: realtimeStage,
      connections: [{
        from: 'File',
        to: 'Wrangler'
      }]
    };
  }

  constructDatabaseSource(artifactsList, dbInfo) {
    if (!dbInfo) { return null; }

    let batchArtifact = find(artifactsList, { 'name': 'database-plugins' });
    if (!batchArtifact) {
      return T.translate(`${PREFIX}.databasePipeline.pipelineError`);
    }
    batchArtifact.version = '[1.7.0, 3.0.0)';

    let plugin = objectQuery(dbInfo, 'values', 0, 'Database');

    let pluginInfo = {
      name: 'Database',
      label: plugin.name,
      type: 'batchsource',
      artifact: batchArtifact,
      properties: plugin.properties
    };

    let batchStage = {
      name: plugin.name,
      plugin: pluginInfo
    };

    return {
      batchSource: batchStage,
      connections: [{
        from: plugin.name,
        to: 'Wrangler'
      }]
    };
  }

  constructKafkaSource(artifactsList, kafkaInfo) {
    if (!kafkaInfo) { return null; }

    let plugin = objectQuery(kafkaInfo, 'values', '0');
    let pluginName = Object.keys(plugin)[0];

    // This is a hack.. should not do this
    // We are still shipping kafka-plugins with hydrator-plugins 1.7 but
    // it doesn't contain the streamingsource or batchsource plugins
    let pluginArtifact = find(artifactsList, {name: 'kafka-plugins'});
    if (!pluginArtifact) {
      return T.translate(`${PREFIX}.kafkaPipeline.pipelineError`);
    }

    plugin = plugin[pluginName];

    plugin.properties.schema = "{\"name\":\"kafkaAvroSchema\",\"type\":\"record\",\"fields\":[{\"name\":\"body\",\"type\":\"string\"}]}";
    let batchPluginInfo = {
      name: plugin.name,
      label: plugin.name,
      type: 'batchsource',
      artifact: pluginArtifact,
      properties: plugin.properties
    };

    let realtimePluginInfo = Object.assign({}, batchPluginInfo, {
      type: 'streamingsource',
      artifact: pluginArtifact
    });

    let batchStage = {
      name: plugin.name,
      plugin: batchPluginInfo
    };

    let realtimeStage = {
      name: plugin.name,
      plugin: realtimePluginInfo
    };

    return {
      batchSource: batchStage,
      realtimeSource: realtimeStage,
      connections: [{
        from: plugin.name,
        to: 'Wrangler'
      }]
    };
  }

  constructS3Source(artifactsList, s3Info) {
    if (!s3Info) {
      return null;
    }
    let batchArtifact = find(artifactsList, {name: 'amazon-s3-plugins'});
    if (!batchArtifact) {
      this.setState({
        error: T.translate(`${PREFIX}.S3Pipeline.pipelineError`),
        loading: false
      });
      return;
    }
    batchArtifact.version = '[1.7.0, 3.0.0)';
    let plugin = objectQuery(s3Info, 'values', 0, 'S3');
    let batchPluginInfo = {
      name: plugin.name,
      label: plugin.name,
      type: 'batchsource',
      artifact: batchArtifact,
      properties: {...plugin.properties, referenceName: plugin.name}
    };
    let batchStage = {
      name: 'S3',
      plugin: batchPluginInfo
    };
    return {
      batchSource: batchStage,
      connections: [{
        from: 'S3',
        to: 'Wrangler'
      }]
    };
  }

  constructGCSSource(artifactsList, gcsInfo) {
    if (!gcsInfo) { return null; }

    let batchArtifact = find(artifactsList, {name: 'google-cloud'});
    if (!batchArtifact) {
      this.setState({
        error: T.translate(`${PREFIX}.GCSPipeline.pipelineError`),
        loading: false
      });
      return;
    }

    batchArtifact.version = '[0.9.0, 3.0.0)';
    let plugin = objectQuery(gcsInfo, 'values', 0);

    let pluginName = Object.keys(plugin)[0]; // this is because the plugin can be GCSFile or GCSFileBlob

    plugin = plugin[pluginName];

    let batchPluginInfo = {
      name: plugin.name,
      label: plugin.name,
      type: 'batchsource',
      artifact: batchArtifact,
      properties: plugin.properties
    };

    let batchStage = {
      name: 'GCS',
      plugin: batchPluginInfo
    };

    return {
      batchSource: batchStage,
      connections: [{
        from: 'GCS',
        to: 'Wrangler'
      }]
    };
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

    let rxArray = [
      MyDataPrepApi.getSchema(requestObj, requestBody)
    ];

    if (state.workspaceInfo.properties.connection === 'file') {
      let specParams = {
        namespace,
        path: state.workspaceUri
      };

      rxArray.push(MyDataPrepApi.getSpecification(specParams));
    } else if (state.workspaceInfo.properties.connection === 'database') {
      let specParams = {
        namespace,
        connectionId: state.workspaceInfo.properties.connectionid,
        tableId: state.workspaceInfo.properties.id
      };

      rxArray.push(MyDataPrepApi.getDatabaseSpecification(specParams));
    } else if (state.workspaceInfo.properties.connection === 'kafka') {
      let specParams = {
        namespace,
        connectionId: state.workspaceInfo.properties.connectionid,
        topic: state.workspaceInfo.properties.topic
      };

      rxArray.push(MyDataPrepApi.getKafkaSpecification(specParams));
    } else if (state.workspaceInfo.properties.connection === 's3') {
      let specParams = {
        namespace,
        connectionId: state.workspaceInfo.properties.connectionid,
        activeBucket: state.workspaceInfo.properties['bucket-name'],
        key: state.workspaceInfo.properties.key
      };

      rxArray.push(MyDataPrepApi.getS3Specification(specParams));
    } else if (state.workspaceInfo.properties.connection === 'gcs') {
      let specParams = {
        namespace,
        connectionId: state.workspaceInfo.properties.connectionid,
        wid: workspaceId
      };

      rxArray.push(MyDataPrepApi.getGCSSpecification(specParams));
    }

    MyArtifactApi.list({ namespace })
      .combineLatest(rxArray)
      .subscribe((res) => {
        let batchArtifactsList = res[0].filter((artifact) => {
          return artifact.name === 'cdap-data-pipeline';
        });
        let realtimeArtifactsList = res[0].filter((artifact) => {
          return artifact.name === 'cdap-data-streams';
        });

        let highestBatchArtifactVersion = findHighestVersion(batchArtifactsList.map((artifact) => artifact.version), true);
        let highestRealtimeArtifactVersion = findHighestVersion(realtimeArtifactsList.map((artifact) => artifact.version), true);

        let batchArtifact = {
          name: 'cdap-data-pipeline',
          version: highestBatchArtifactVersion,
          scope: 'SYSTEM'
        };

        let realtimeArtifact = {
          name: 'cdap-data-streams',
          version: highestRealtimeArtifactVersion,
          scope: 'SYSTEM'
        };

        let wranglerArtifact = this.findWranglerArtifacts(res[0], pluginVersion);

        let tempSchema = {
          name: 'avroSchema',
          type: 'record',
          fields: res[1]
        };

        let properties = {
          workspaceId,
          directives: directives.join('\n'),
          schema: JSON.stringify(tempSchema),
          field: '*'
        };

        if (state.workspaceInfo.properties.connection === 'file') {
          properties.field = 'body';
        }

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

        let wranglerStage = {
          name: 'Wrangler',
          plugin: {
            name: 'Wrangler',
            label: 'Wrangler',
            type: 'transform',
            artifact: wranglerArtifact,
            properties
          }
        };

        let connections = [];

        let realtimeStages = [wranglerStage];
        let batchStages = [wranglerStage];

        let sourceConfigs;
        if (state.workspaceInfo.properties.connection === 'file') {
          sourceConfigs = this.constructFileSource(res[0], res[2]);
        } else if (state.workspaceInfo.properties.connection === 'database') {
          sourceConfigs = this.constructDatabaseSource(res[0], res[2]);
        } else if (state.workspaceInfo.properties.connection === 'kafka') {
          sourceConfigs = this.constructKafkaSource(res[0], res[2]);
        } else if (state.workspaceInfo.properties.connection === 's3') {
          sourceConfigs = this.constructS3Source(res[0], res[2]);
        } else if (state.workspaceInfo.properties.connection === 'gcs') {
          sourceConfigs = this.constructGCSSource(res[0], res[2]);
        }

        if (typeof sourceConfigs === 'string') {
          this.setState({
            error: sourceConfigs,
            loading: false
          });
          return;
        }

        if (sourceConfigs) {
          realtimeStages.push(sourceConfigs.realtimeSource);
          batchStages.push(sourceConfigs.batchSource);
          connections = sourceConfigs.connections;
        }

        let realtimeConfig = {
          artifact: realtimeArtifact,
          config: {
            stages: realtimeStages,
            batchInterval: '10s',
            connections
          }
        };

        let batchConfig = {
          artifact: batchArtifact,
          config: {
            stages: batchStages,
            connections
          }
        };

        let realtimeUrl = window.getHydratorUrl({
          stateName: 'hydrator.create',
          stateParams: {
            namespace,
            workspaceId,
            artifactType: realtimeArtifact.name
          }
        });

        if (['database', 's3', 'gcs'].indexOf(state.workspaceInfo.properties.connection) !== -1) {
          realtimeUrl = null;
        }

        let batchUrl = window.getHydratorUrl({
          stateName: 'hydrator.create',
          stateParams: {
            namespace,
            workspaceId,
            artifactType: batchArtifact.name
          }
        });

        this.setState({
          loading: false,
          realtimeUrl,
          batchUrl,
          workspaceId,
          realtimeConfig,
          batchConfig
        });

      }, (err) => {
        console.log('err', err);

        this.setState({
          error: objectQuery(err, 'response', 'message')  || T.translate(`${PREFIX}.PipelineModal.defaultErrorMessage`),
          loading: false
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
          }, () => {
            this.generateLinks();
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

  renderInvalidFieldError() {
    if (!objectQuery(this.state, 'error', 'remedies')) { return null; }

    return (
      <pre>
        <div className="remedy-message">
          {
            objectQuery(this.state, 'error', 'remedies') ? this.state.error.remedies : null
          }
        </div>
        <span>
          {T.translate(`${PREFIX}.invalidFieldNameRemedies2`)}
          <span
            className="btn-link"
            onClick={this.applyDirective.bind(this, 'cleanse-column-names')}
          >
            {T.translate(`${PREFIX}.cleanseLinkLabel`)}
          </span>
          {T.translate(`${PREFIX}.invalidFieldNameRemedies3`)}
        </span>
      </pre>
    );
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
    } else {
      let realtimeDisabledTooltip;

      if (!this.state.realtimeUrl) {
        realtimeDisabledTooltip = T.translate(`${PREFIX}.realtimeDisabledTooltip`);
      }

      content = (
        <div>
          <div className="message">
            {T.translate(`${PREFIX}.addToPipelineModal.title`)}
          </div>
          <div className="action-buttons">
            <a
              href={this.state.error ? null : this.state.batchUrl}
              className={classnames('btn btn-secondary', {
                'inactive': this.state.error
              })}
              onClick={(() => {
                if (this.state.error) { return; }
                window.localStorage.setItem(this.state.workspaceId, JSON.stringify(this.state.batchConfig));
              }).bind(this)}
            >
              <i className="fa icon-ETLBatch"/>
              <span>{T.translate(`${PREFIX}.addToPipelineModal.batchPipelineBtn`)}</span>
            </a>
            <a
              href={this.state.realtimeUrl}
              className={classnames('btn btn-secondary', {
                'inactive': !this.state.realtimeUrl || this.state.error
              })}
              onClick={(() => {
                if (!this.state.realtimeUrl) { return; }
                window.localStorage.setItem(this.state.workspaceId, JSON.stringify(this.state.realtimeConfig));
              }).bind(this)}
              title={realtimeDisabledTooltip}
            >
              <i className="fa icon-sparkstreaming"/>
              <span>{T.translate(`${PREFIX}.addToPipelineModal.realtimePipelineBtn`)}</span>
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
            {T.translate(`${PREFIX}.addToPipelineBtnLabel`)}
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
          {this.renderInvalidFieldError()}
        </ModalBody>
        {
          this.state.error ?
            <ModalFooter className="dataset-copy-error-container">
              <CardActionFeedback
                type='DANGER'
                message={T.translate(`${PREFIX}.addToPipelineModal.errorTitle`)}
                extendedMessage={this.state.error}
              />
            </ModalFooter>
          :
            null
        }
      </Modal>
    );
  }
}

AddToHydratorModal.propTypes = {
  toggle: PropTypes.func
};
