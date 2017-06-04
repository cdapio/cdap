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
import T from 'i18n-react';
import {Modal, ModalHeader, ModalBody, ModalFooter, Button, ButtonGroup, Form, FormGroup, Label, Col, Input} from 'reactstrap';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import {preventPropagation} from 'services/helpers';
import DataPrepStore from 'components/DataPrep/store/';
import GetPipelineConfig from 'components/DataPrep/TopPanel/PipelineConfigHelper';
import {MyArtifactApi} from 'api/artifact';
import {MyDatasetApi} from 'api/dataset';
import {MyAppApi} from 'api/app';
import {MyProgramApi} from 'api/program';
import NamespaceStore from 'services/NamespaceStore';
import find from 'lodash/find';
import {objectQuery} from 'services/helpers';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';
import cloneDeep from 'lodash/cloneDeep';
import Rx from 'rx';

require('./CreateDatasetBtn.scss');

const PREFIX = `features.DataPrep.TopPanel.copyToCDAPDatasetBtn`;
const fielsetDataType = [
  {
    id: 'TPFSAvro',
    label: T.translate(`${PREFIX}.Formats.avro`)
  },
  {
    id: 'TPFSOrc',
    label: T.translate(`${PREFIX}.Formats.orc`)
  },
  {
    id: 'TPFSParquet',
    label: T.translate(`${PREFIX}.Formats.parquet`)
  }
];
const copyingSteps = [
  {
    message: T.translate(`${PREFIX}.copyingSteps.Step1`),
    error: T.translate(`${PREFIX}.copyingSteps.Step1Error`),
    status: null
  },
  {
    message: T.translate(`${PREFIX}.copyingSteps.Step2`),
    error: T.translate(`${PREFIX}.copyingSteps.Step2Error`),
    status: null
  }
];
export default class CreateDatasetBtn extends Component {
  constructor(props) {
    super(props);
    this.state = this.getDefaultState();
    this.toggleModal = this.toggleModal.bind(this);
    this.handleOnSubmit = this.handleOnSubmit.bind(this);
    this.handleRowkeyChange = this.handleRowkeyChange.bind(this);
    this.submitForm = this.submitForm.bind(this);
    this.handleFormatChange = this.handleFormatChange.bind(this);
    this.handleDatasetNameChange = this.handleDatasetNameChange.bind(this);
  }

  getDefaultState() {
    let {headers} = DataPrepStore.getState().dataprep;
    return {
      showModal: false,
      inputType: 'fileset',
      rowKey: headers.length ? headers[0] : null,
      format: fielsetDataType[0].id,
      sinkPluginsForDataset: {},
      batchPipelineConfig: {},
      datasetName: '',
      copyingSteps: [...copyingSteps],
      copyInProgress: false,
      // This is to enable closing the modal on workflow start.
      // Ideally users won't wait till the dataset is created (till pipeline runs successfully and creates the dataset)
      copyTaskStarted: false,
      datasetUrl: null,
      error: null
    };
  }

  componentWillMount() {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let corePlugins;
    MyArtifactApi
      .list({ namespace })
      .subscribe(res => {
        corePlugins = find(res, { 'name': 'core-plugins' });
        const getPluginConfig = (pluginName) => {
          return {
            name: pluginName,
            plugin: {
              name: pluginName,
              label: pluginName,
              type: 'batchsink',
              artifact: corePlugins,
              properties: {}
            }
          };
        };
        let sinks = {
          TPFSAvro: getPluginConfig('TPFSAvro'),
          TPFSParquet: getPluginConfig('TPFSParquet'),
          TPFSOrc: getPluginConfig('TPFSOrc'),
          Table: getPluginConfig('Table'),
        };
        this.setState({
          sinkPluginsForDataset: sinks
        });
      });
  }

  toggleModal() {
    let state = Object.assign(this.getDefaultState(), {
      showModal: !this.state.showModal,
      sinkPluginsForDataset: this.state.sinkPluginsForDataset,
      batchPipelineConfig: this.state.batchPipelineConfig
    });
    this.setState(state);
    if (!this.state.showModal) {
      GetPipelineConfig().subscribe(
        (res) => {
          this.setState({
            batchPipelineConfig: res.batchConfig
          });
        },
        (err) => {
          this.setState({
            error: err
          });
        }
      );
    }
  }

  handleDatasetNameChange(e) {
    this.setState({
      datasetName: e.target.value
    });
  }

  handleRowkeyChange(e) {
    this.setState({
      rowKey: e.target.value
    });
  }

  handleFormatChange(e) {
    this.setState({
      format: e.target.value
    });
  }

  handleOnSubmit(e) {
    preventPropagation(e);
    return false;
  }

  getAppConfigMacros () {
    let {workspaceInfo, directives, headers} = DataPrepStore.getState().dataprep;
    let pipelineConfig = cloneDeep(this.state.batchPipelineConfig);
    let wranglerStage = pipelineConfig.config.stages.find(stage => stage.name === 'Wrangler');
    let dbStage = pipelineConfig.config.stages.find(stage => stage.name === 'Database');
    let databaseConfig = objectQuery(workspaceInfo, 'properties', 'databaseConfig');
    let macroMap = {};
    if (databaseConfig) {
      try {
        databaseConfig = JSON.parse(databaseConfig);
      } catch (e) {
        databaseConfig = {};
      }
      macroMap = Object.assign(macroMap, databaseConfig);
    }
    return Object.assign({}, macroMap, {
      datasetName: this.state.datasetName,
      filename: objectQuery(workspaceInfo, 'properties', 'path') || '',
      directives: directives.join('\n'),
      schema: objectQuery(wranglerStage, 'plugin', 'properties', 'schema') || '',
      schemaRowField: isNil(this.state.rowKey) ? headers[0] : this.state.rowKey,
      query: objectQuery(dbStage, 'plugin', 'properties', 'importQuery') || '',
      connectionString: objectQuery(dbStage, 'plugin', 'properties', 'connectionString') || '',
      password: objectQuery(dbStage, 'plugin', 'properties', 'password') || '',
      userName: objectQuery(dbStage, 'plugin', 'properties', 'user') || ''
    });
  }

  addMacrosToPipelineConfig(pipelineConfig) {
    let {dataprep} = DataPrepStore.getState();
    let workspaceProps = objectQuery(dataprep, 'workspaceInfo', 'properties');

    let macroMap = this.getAppConfigMacros();
    let dataFormatProperties = {
      schema: '${schema}',
      name: '${datasetName}'
    };
    let pluginsMap = {
      'Wrangler': {
        directives: '${directives}',
        schema: '${schema}',
        field: workspaceProps.connection === 'file' ? 'body' : '*',
        precondition: 'false',
        'threshold': '1',
      },
      'File': {
        path: '${filename}',
        referenceName: 'FileNode',
        schema: "{\"name\":\"fileRecord\",\"type\":\"record\",\"fields\":[{\"name\":\"offset\",\"type\":\"long\"},{\"name\":\"body\",\"type\":\"string\"}]}"
      },
      'Table': {
        'schema.row.field': '${schemaRowField}',
        name: '${datasetName}',
        schema: '${schema}'
      },
      'Database': {
        connectionString: '${connectionString}',
        user: '${userName}',
        password: '${password}',
        importQuery: '${query}'
      },
      'TPFSOrc': dataFormatProperties,
      'TPFSParquet': dataFormatProperties,
      'TPFSAvro': dataFormatProperties
    };
    pipelineConfig.config.stages = pipelineConfig.config.stages.map(stage => {
      if (!isNil(pluginsMap[stage.name])) {
        stage.plugin.properties = Object.assign({}, stage.plugin.properties, pluginsMap[stage.name]);
      }
      return stage;
    });
    return {pipelineConfig, macroMap};
  }

  preparePipelineConfig() {
    let sink;
    let {workspaceInfo} = DataPrepStore.getState().dataprep;
    let {name: pipelineName} = workspaceInfo.properties;
    let pipelineconfig = cloneDeep(this.state.batchPipelineConfig);
    if (this.state.inputType === 'fileset') {
      sink = fielsetDataType.find(dataType => dataType.id === this.state.format);
      if (sink) {
        sink = this.state.sinkPluginsForDataset[sink.id];
      }
    }
    if (this.state.inputType === 'table') {
      sink = this.state.sinkPluginsForDataset['Table'];
    }
    pipelineconfig.config.stages.push(sink);
    let {pipelineConfig: appConfig, macroMap} = this.addMacrosToPipelineConfig(pipelineconfig);
    let connections = this.state.batchPipelineConfig.config.connections;
    let sinkConnection = [
      {
        from: connections[0].to,
        to: sink.name
      }
    ];
    appConfig.config.connections = connections.concat(sinkConnection);
    appConfig.config.schedule = '0 * * * *';
    appConfig.config.engine = 'mapreduce';
    appConfig.description = `Pipeline to create dataset for workspace ${pipelineName} from dataprep`;
    return {appConfig, macroMap};
  }

  submitForm() {
    let steps = cloneDeep(copyingSteps);
    let {dataprep} = DataPrepStore.getState();
    let workspaceProps = objectQuery(dataprep, 'workspaceInfo', 'properties');
    steps[0].status = 'running';
    this.setState({
      copyInProgress: true,
      copyingSteps: steps
    });
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let pipelineName;
    // FIXME: We need to fix backend to enable adding macro to pluginType and name in database.
    // Right now we don't support it and hence UI creates new pipeline based on jdbc plugin name.
    let dbStage = this.state.batchPipelineConfig.config.stages.find(dataType => dataType.name === 'Database');
    if (this.state.inputType === 'fileset') {
      pipelineName = `one_time_copy_to_fs_${this.state.format}`;
      if (workspaceProps.connection === 'database') {
        pipelineName = `one_time_copy_to_fs_from_${dbStage.plugin.properties.jdbcPluginName}`;
      }
    } else {
      pipelineName = `one_time_copy_to_table`;
      if (workspaceProps.connection === 'database') {
        pipelineName = `one_time_copy_to_table_from_${dbStage.plugin.properties.jdbcPluginName}`;
      }
    }

    pipelineName = pipelineName.replace('TPFS', '');
    pipelineName = pipelineName.toLowerCase();
    let pipelineconfig, macroMap;

    // Get list of pipelines to check if the pipeline is already published
    MyAppApi.list({namespace})
      .flatMap(res => {
        let appAlreadyDeployed = res.find(app => app.id === pipelineName);
        if (!appAlreadyDeployed) {
          let appConfigWithMacros= this.preparePipelineConfig();
          pipelineconfig = appConfigWithMacros.appConfig;
          macroMap = appConfigWithMacros.macroMap;
          pipelineconfig.name = pipelineName;
          let params = {
            namespace,
            appId: pipelineName
          };
          // If it doesn't exist create a new pipeline with macros.
          return MyAppApi.deployApp(params, pipelineconfig);
        }
        // If it already exists just move to next step.
        return Rx.Observable.create( observer => {
          observer.next();
        });
      })
      .flatMap(
        () => {
          let copyingSteps = [...this.state.copyingSteps];
          copyingSteps[0].status = 'success';
          copyingSteps[1].status = 'running';
          this.setState({
            copyingSteps
          });
          if (!macroMap) {
            macroMap = this.getAppConfigMacros();
          }
          // Once the pipeline is published start the workflow. Pass run time arguments for macros.
          return MyProgramApi.action({
            namespace,
            appId: pipelineName,
            programType: 'workflows',
            programId: 'DataPipelineWorkflow',
            action: 'start'
          }, macroMap);
        }
      )
      .flatMap(
        () => {
          this.setState({
            copyTaskStarted: true
          });
          let count = 1;
          const getDataset = (callback, errorCallback, count) => {
            let params = {
              namespace,
              datasetId: this.state.datasetName
            };
            MyDatasetApi
              .get(params)
              .subscribe(
                callback,
                () => {
                  if (count < 120) {
                    count += count;
                    setTimeout(() => {
                      getDataset(callback, errorCallback, count);
                    }, count * 1000);
                  } else {
                    errorCallback();
                  }
                }
              );
          };
          return Rx.Observable.create((observer) => {
            let successCallback = () => {
              observer.onNext();
            };
            let errorCallback = () => {
              observer.onError('Copy task timed out after 2 mins. Please check logs for more information.');
            };
            getDataset(successCallback, errorCallback, count);
          });
        }
      )
      .subscribe(
        () => {
          // Once workflow started successfully create a link to pipeline datasets tab for user reference.
          let copyingSteps = [...this.state.copyingSteps];
          let {selectedNamespace: namespaceId} = NamespaceStore.getState();
          copyingSteps[1].status = 'success';
          let datasetUrl = window.getAbsUIUrl({
            namespaceId,
            entityType: 'datasets',
            entityId: this.state.datasetName
          });
          datasetUrl = `${datasetUrl}?modalToOpen=explore`;
          this.setState({
            copyingSteps,
            datasetUrl
          });
        },
        (err) => {
          let copyingSteps = this.state.copyingSteps.map((step) => {
            if (step.status === 'running') {
              return Object.assign({}, step, {status: 'failure'});
            }
            return step;
          });
          let state = {
            copyingSteps
          };
          if (!this.state.error) {
            state.error = typeof err === 'object' ? err.response : err;
          }
          this.setState(state);
        }
      ); // FIXME: Need to handle the failure case here as well.
  }

  setType(type) {
    this.setState({
      inputType: type
    });
  }

  renderDatasetSpecificContent() {
    if (this.state.inputType === 'table') {
      let {headers} = DataPrepStore.getState().dataprep;
      return (
        <FormGroup row>
          <Label
            xs="4"
            className="text-xs-right"
          >
            {T.translate(`${PREFIX}.Form.rowKeyLabel`)}
            <span className="text-danger">*</span>
          </Label>
          <Col xs="6">
            <Input
              type="select"
              onChange={this.handleRowkeyChange}
              value={this.state.rowKey}
            >
              {
                headers.map(header => {
                  return (
                    <option value={header}>{header}</option>
                  );
                })
              }
            </Input>
            <IconSVG
              id="row-key-info-icon"
              name="icon-info-circle"
            />
            <UncontrolledTooltip target="row-key-info-icon" delay={{show: 250, hide: 0}}>
              {T.translate(`${PREFIX}.Form.rowKeyTooltip`)}
            </UncontrolledTooltip>
          </Col>
        </FormGroup>
      );
    }
    if (this.state.inputType === 'fileset') {
      return (
        <FormGroup row>
          <Label
            xs="4"
            className="text-xs-right"
          >
            {T.translate(`${PREFIX}.Form.formatLabel`)}
            <span className="text-danger">*</span>
          </Label>
          <Col xs="6">
            <Input
              type="select"
              onChange={this.handleFormatChange}
              value={this.state.format}
            >
              {
                fielsetDataType.map(datatype => {
                  return (
                    <option value={datatype.id}>{datatype.label}</option>
                  );
                })
              }
            </Input>
            <IconSVG
              id="row-key-info-icon"
              name="icon-info-circle"
            />
            <UncontrolledTooltip target="row-key-info-icon" delay={{show: 250, hide: 0}}>
              {T.translate(`${PREFIX}.Form.formatTooltip`)}
            </UncontrolledTooltip>
          </Col>
        </FormGroup>
      );
    }
  }

  renderSteps() {
    if (!this.state.copyInProgress) {
      return null;
    }
    const statusContainer = (status) => {
      let icon, className;
      if (status === 'running') {
        icon="icon-spinner";
        className="fa-spin";
      }
      if (status === 'success') {
        icon="icon-check-circle";
      }
      if (status === 'failure') {
        icon="icon-times-circle";
      }
      return (
        <IconSVG
          name={icon}
          className={className}
        />
      );
    };
    return (
      <div className="text-xs-left steps-container">
        {
          this.state.copyingSteps.map(step => {
            return (
              <div className={classnames("step-container", {
                "text-success": step.status === 'success',
                "text-danger": step.status === 'failure',
                "text-info": step.status === 'running',
                "text-muted": step.status === null
              })}>
                <span>
                  {statusContainer(step.status)}
                </span>
                <span>
                  {
                    step.status === 'failure' ? step.error : step.message
                  }
                </span>
              </div>
            );
          })
        }
        {
          this.state.copyingSteps[1].status === 'success' ?
            <a
              className="btn btn-primary"
              href={`${this.state.datasetUrl}`}
            >
              {T.translate(`${PREFIX}.monitorBtnLabel`)}
            </a>
          :
            null
        }
      </div>
    );
  }

  renderForm() {
    let {dataprep} = DataPrepStore.getState();
    let isTableOptionDisabled = objectQuery(dataprep, 'workspaceInfo', 'properties', 'databaseConfig');
    return (
      <div>
        <p>{T.translate(`${PREFIX}.description`)}</p>
        <Form onSubmit={this.handleOnSubmit}>
          <FormGroup row>
            <Label
              xs={4}
              className="text-xs-right"
            >
              {T.translate(`${PREFIX}.Form.typeLabel`)}
            </Label>
            <Col xs={8}>
              <ButtonGroup className="input-type-group">
                <Button
                  color="secondary"
                  onClick={this.setType.bind(this, 'fileset')}
                  active={this.state.inputType === 'fileset'}
                >
                  {T.translate(`${PREFIX}.Form.fileSetBtnlabel`)}
                </Button>
                <Button
                  color="secondary"
                  onClick={this.setType.bind(this, 'table')}
                  active={this.state.inputType === 'table'}
                  disabled={isNil(isTableOptionDisabled) ? false : true}
                >
                  {T.translate(`${PREFIX}.Form.tableBtnlabel`)}
                </Button>
              </ButtonGroup>
            </Col>
          </FormGroup>
          <FormGroup row>
            <Col xs="4"></Col>
            <Col xs="8"></Col>
          </FormGroup>
          <FormGroup row>
            <Label
              xs="4"
              className="text-xs-right"
            >
              {T.translate(`${PREFIX}.Form.datasetNameLabel`)}
              <span className="text-danger">*</span>
            </Label>
            <Col xs="6" className="dataset-name-group">
              <p className="required-label">
                {T.translate(`${PREFIX}.Form.requiredLabel`)}
                <span className="text-danger">*</span>
              </p>
              <Input
                value={this.state.datasetName}
                onChange={this.handleDatasetNameChange}
              />
              <IconSVG
                id="dataset-name-info-icon"
                name="icon-info-circle"
              />
              <UncontrolledTooltip target="dataset-name-info-icon" delay={{show: 250, hide: 0}}>
                {T.translate(`${PREFIX}.Form.datasetTooltip`)}
              </UncontrolledTooltip>
            </Col>
          </FormGroup>
          {this.renderDatasetSpecificContent()}
        </Form>
      </div>
    );
  }

  renderFooter() {
    if (!this.state.copyInProgress) {
      return (
        <ModalFooter>
          <button
            className="btn btn-primary"
            onClick={this.submitForm}
            disabled={isEmpty(this.state.datasetName)}
          >
            {T.translate(`${PREFIX}.createBtnLabel`)}
          </button>
          <button
            className="btn btn-secondary"
            onClick={this.toggleModal}
          >
            {T.translate('features.DataPrep.Directives.cancel')}
          </button>
          {
            this.renderSteps()
          }
        </ModalFooter>
      );
    }
    if (this.state.error) {
      return (
        <ModalFooter className="dataset-copy-error-container">
          <div className="step-error-container">
            {
              this.state.error
            }
          </div>
        </ModalFooter>
      );
    }
  }
  render() {
    return (
      <span className="create-dataset-btn" title={this.props.title}>
        <button
          className={classnames("btn btn-link", this.props.className)}
          onClick={this.toggleModal}
          disabled={this.props.disabledState}
        >
          {T.translate(`${PREFIX}.btnLabel`)}
        </button>
        <Modal
          toggle={this.toggleModal}
          isOpen={this.state.showModal}
          size="md"
          backdrop="static"
          keyboard={false}
          zIndex="1061"
          className="dataprep-parse-modal create-dataset-modal"
        >
          <ModalHeader>
            <span>
              {T.translate(`${PREFIX}.modalTitle`)}
            </span>

            <div
              className={classnames("close-section float-xs-right", {
                "disabled": this.state.copyInProgress && !this.state.copyTaskStarted && !this.state.error
              })}
              onClick={this.state.copyInProgress && !this.state.copyTaskStarted && !this.state.error? () => {} : this.toggleModal}
            >
              <span className="fa fa-times" />
            </div>
          </ModalHeader>
          <ModalBody className={classnames({
            "copying-steps-container": this.state.copyInProgress
          })}>
            {
              this.state.copyInProgress ?
                this.renderSteps()
              :
                this.renderForm()
            }
          </ModalBody>
          {
            this.renderFooter()
          }
        </Modal>
      </span>
    );
  }
}
CreateDatasetBtn.propTypes = {
  className: PropTypes.string,
  disabledState: PropTypes.bool,
  title: PropTypes.string
};
