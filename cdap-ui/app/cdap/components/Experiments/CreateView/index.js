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
import React, {Component} from 'react';
import TopPanel from 'components/Experiments/TopPanel';
import IconSVG from 'components/IconSVG';
import DataPrepConnections from 'components/DataPrepConnections';
import DataPrepHome from 'components/DataPrepHome';
import {Prompt, Link, Redirect} from 'react-router-dom';
import createExperimentStore, {CREATION_STEPS} from 'components/Experiments/store/createExperimentStore';
import {getCurrentNamespace} from 'services/NamespaceStore';
import Popover from 'components/Popover';
import ExperimentPopovers from 'components/Experiments/CreateView/Popovers';
import DataPrepStore from 'components/DataPrep/store';
import {
  setOutcomeColumns,
  setDirectives,
  setSrcPath,
  setWorkspace,
  updateSchema,
  getExperimentForEdit,
  getExperimentModelSplitForCreate,
  resetCreateExperimentsStore,
  fetchAlgorithmsList,
  setExperimentCreateError,
  setAlgorithmsListForCreateView,
  updateModel
} from 'components/Experiments/store/CreateExperimentActionCreator';
import MLAlgorithmSelection from 'components/Experiments/CreateView/MLAlgorithmSelection';
import SplitDataStep from 'components/Experiments/CreateView/SplitDataStep';
import ExperimentMetadata from 'components/Experiments/CreateView/ExperimentMetadata';
import Helmet from 'react-helmet';
import queryString from 'query-string';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {directiveRequestBodyCreator} from 'components/DataPrep/helper';
import MyDataPrepApi from 'api/dataprep';
import Alert from 'components/Alert';

require('./CreateView.scss');

export default class ExperimentCreateView extends Component {
  static propTypes = {
    match: PropTypes.object,
    location: PropTypes.object,
  };
  state = {
    workspaceId: createExperimentStore.getState().experiments_create.workspaceId,
    modelId: createExperimentStore.getState().model_create.modelId,
    experimentId: createExperimentStore.getState().experiments_create.name,
    isSplitFinalized: createExperimentStore.getState().model_create.isSplitFinalized,
    loading: createExperimentStore.getState().experiments_create.loading,
    active_step: createExperimentStore.getState().active_step.step_name,
    redirectToExperimentDetail: false
  };
  title = 'Create a new experiment';
  componentWillMount() {
    setAlgorithmsListForCreateView();
  }
  componentDidMount() {
    this.dataprepsubscription = DataPrepStore.subscribe(() => {
      let {dataprep} = DataPrepStore.getState();
      let {headers = [], directives, workspaceInfo = {}, workspaceId} = dataprep;
      if (!headers.length) {
        return;
      }
      setWorkspace(workspaceId);
      setSrcPath(workspaceInfo.properties.path);
      setOutcomeColumns(headers);
      setDirectives(directives);
      let requestBody = directiveRequestBodyCreator(directives);
      MyDataPrepApi
        .getSchema({
          namespace: getCurrentNamespace(),
          workspaceId
        }, requestBody)
        .subscribe(updateSchema);
    });
    this.createExperimentStoreSubscription = createExperimentStore.subscribe(() => {
      let {model_create, experiments_create, active_step} = createExperimentStore.getState();
      let {modelId, isSplitFinalized, isModelTrained} = model_create;
      let {workspaceId, loading, name: experimentId, error: experimentError} = experiments_create;
      let newState = {};
      if (this.state.experimentId !== experimentId) {
        newState = {experimentId};
      }
      if (this.state.active_step.step_name !== active_step.step_name) {
        newState = { ...newState, active_step };
      }
      if (this.state.modelId !== modelId) {
        newState = {...newState, modelId};
      }
      if (this.state.workspaceId !== workspaceId) {
        newState = {...newState, workspaceId};
      }
      if (this.state.loading !== loading) {
        newState = {...newState, loading};
      }
      if (isSplitFinalized !== this.state.isSplitFinalized) {
        newState = {...newState, isSplitFinalized};
      }
      if (isModelTrained) {
        newState = {...newState, redirectToExperimentDetail: true};
      }
      if (experimentError) {
        newState = {...newState, experimentError};
      }
      if (Object.keys(newState).length > 0) {
        this.setState(newState);
      }
    });
    let {experimentId, modelId} = queryString.parse(this.props.location.search);
    if (experimentId && !modelId) {
      getExperimentForEdit(experimentId);
    } else if (experimentId && modelId) {
      getExperimentModelSplitForCreate(experimentId, modelId);
    } else {
      this.setState({loading: false});
    }
    fetchAlgorithmsList();
  }
  componentWillUnmount() {
    let {experiments_create} = createExperimentStore.getState();
    let {workspaceId} = experiments_create;
    if (workspaceId) {
      // Every workspace created in experiments create view is temp. So don't worry about deleting it.
      MyDataPrepApi.delete({
        namespace: getCurrentNamespace(),
        workspaceId
      }).subscribe();
    }
    DataPrepStore.dispatch({
      type: DataPrepActions.reset
    });
    if (this.dataprepsubscription) {
      this.dataprepsubscription();
    }
    resetCreateExperimentsStore();
  }
  renderTopPanel = (title) => {
    let navigateTo = `/ns/${getCurrentNamespace()}/experiments`;
    if (this.state.experimentId) {
      navigateTo = `${navigateTo}/${this.state.experimentId}`;
    }
    return (
      <TopPanel>
        <h4>{title}</h4>
        <Link to={navigateTo}>
          <IconSVG name="icon-close" />
        </Link>
      </TopPanel>
    );
  };
  renderConnections() {
    return (
      <span>
        {this.renderTopPanel(this.title)}
        <DataPrepConnections
          sidePanelExpanded={false}
          enableRouting={false}
          singleWorkspaceMode={true}
          onWorkspaceCreate={(workspaceId) => {
            setWorkspace(workspaceId);
            this.setState({workspaceId});
          }}
          scope={true}
          browserTitle="Select a file to use in your experiment"
        />
      </span>
    );
  }
  renderDataPrep() {
    const updateModelBtn = (
      <div className="btn btn-primary" onClick={updateModel}>
        Update Model
      </div>
    );
    const {experimentId, addModel} = queryString.parse(this.props.location.search);
    let popoverElementLabel = 'Create experiment';

    if (addModel) {
      popoverElementLabel = 'Add model';
    }
    const popoverElement = (
      <div className="btn btn-primary">
        {popoverElementLabel}
      </div>
    );
    const createModelBtn = (
      <Popover
        target={() => popoverElement}
        enableInteractionInPopover={true}
        targetDimension={{ width: "auto" }}
        placement="bottom"
        className="create_new_experiment_popover"
        showPopover={!experimentId ? true : false}
      >
        <ExperimentPopovers />
      </Popover>
    );


    let topPanelTitle = 'Create a new experiment';
    if (addModel) {
      topPanelTitle = `Add model to '${experimentId}'`;
    }
    const renderAddBtn = () => {
      if (!this.state.workspaceId) {
        return null;
      }
      return this.state.modelId ? updateModelBtn : createModelBtn;
    };
    return (
      <span>
        {this.renderTopPanel(topPanelTitle)}
        <div className="experiments-model-panel">
          {renderAddBtn()}
        </div>
        <DataPrepHome
          singleWorkspaceMode={true}
          enableRouting={false}
          workspaceId={this.state.workspaceId}
        />
      </span>
    );
  }
  renderSplitDataStep() {
    let {name} = createExperimentStore.getState().model_create;
    return (
      <span className="experiments-split-data-step">
        {this.renderTopPanel(`Split data for model '${name}'`)}
        <ExperimentMetadata />
        <SplitDataStep />
      </span>
    );
  }
  renderAlgorithmSelectionStep() {
    let {name} = createExperimentStore.getState().model_create;
    return (
      <span className="experiments-algorithm-selection-step">
        {this.renderTopPanel(`Select an algorithm to train model '${name}'`)}
        <ExperimentMetadata />
        <MLAlgorithmSelection />
      </span>
    );
  }
  renderSteps() {
    if (this.state.loading) {
      return <LoadingSVGCentered />;
    }
    if (this.state.redirectToExperimentDetail) {
      return (
        <Redirect
          to={`/ns/${getCurrentNamespace()}/experiments/${this.state.experimentId}`}
        />
      );
    }
    switch (this.state.active_step.step_name) {
      case CREATION_STEPS.DATAPREP_CONNECTIONS:
        return this.renderConnections();
      case CREATION_STEPS.DATAPREP:
        return this.renderDataPrep();
      case CREATION_STEPS.DATASPLIT:
        return this.renderSplitDataStep();
      case CREATION_STEPS.ALGORITHM_SELECTION:
        return this.renderAlgorithmSelectionStep();
      default:
        return null;
    }
  }
  renderError() {
    if (!this.state.experimentError) {
      return null;
    }

    return (
      <Alert
        message={this.state.experimentError}
        type='error'
        showAlert={true}
        onClose={setExperimentCreateError}
      />
    );
  }
  render() {
    return (
      <div className="experiments-create-view">
        <Helmet title="CDAP | Create Experiment" />
        {this.renderSteps()}
        {this.renderError()}
        <Prompt
          when={!this.state.experimentId || !this.state.modelId}
          message={"Are you sure you want to navigate away?"}
        />
      </div>
    );
  }
}
