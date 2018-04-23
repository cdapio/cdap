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
import createExperimentStore from 'components/Experiments/store/createExperimentStore';
import {getCurrentNamespace} from 'services/NamespaceStore';
import UncontrolledPopover from 'components/UncontrolledComponents/Popover';
import ExperimentPopovers from 'components/Experiments/CreateView/Popovers';
import DataPrepStore from 'components/DataPrep/store';
import {
  setOutcomeColumns,
  setDirectives,
  setSrcPath,
  setWorkspace,
  getExperimentForEdit,
  getExperimentModelSplitForCreate,
  resetCreateExperimentsStore,
  fetchAlgorithmsList
} from 'components/Experiments/store/CreateExperimentActionCreator';
import {setAlgorithmsList} from 'components/Experiments/store/ActionCreator';
import MLAlgorithmSelection from 'components/Experiments/CreateView/MLAlgorithmSelection';
import SplitDataStep from 'components/Experiments/CreateView/SplitDataStep';
import ExperimentMetadata from 'components/Experiments/CreateView/ExperimentMetadata';
import Helmet from 'react-helmet';
import queryString from 'query-string';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import MyDataPrepApi from 'api/dataprep';

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
    redirectToExperimentDetail: false
  };
  componentWillMount() {
    setAlgorithmsList();
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
    });
    this.createExperimentStoreSubscription = createExperimentStore.subscribe(() => {
      let {model_create, experiments_create} = createExperimentStore.getState();
      let {modelId, isSplitFinalized, isModelTrained} = model_create;
      let {workspaceId, loading, name: experimentId} = experiments_create;
      let newState = {};
      if (this.state.experimentId !== experimentId) {
        newState = {experimentId};
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
        {this.renderTopPanel('Create a New Experiment')}
        <DataPrepConnections
          sidePanelExpanded={true}
          enableRouting={false}
          singleWorkspaceMode={true}
          onWorkspaceCreate={(workspaceId) => {
            setWorkspace(workspaceId);
            this.setState({workspaceId});
          }}
          scope={true}
        />
      </span>
    );
  }
  renderDataPrep() {
    let popoverElement = (
      <div className="btn btn-primary">
        Add a model
      </div>
    );
    return (
      <span>
        {this.renderTopPanel('Create a New Experiment')}
        <div className="experiments-model-panel">
          <UncontrolledPopover
            popoverElement={popoverElement}
            tag="div"
            tetherOption={{
              classPrefix: 'create_new_experiment_popover',
            }}
          >
            <ExperimentPopovers />
          </UncontrolledPopover>
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
    let {name} = createExperimentStore.getState().experiments_create;
    return (
      <span className="experiments-split-data-step">
        {this.renderTopPanel(`Add a Model to '${name}'`)}
        <ExperimentMetadata />
        <SplitDataStep />
      </span>
    );
  }
  renderAlgorithmSelectionStep() {
    let {name} = createExperimentStore.getState().experiments_create;
    return (
      <span className="experiments-algorithm-selection-step">
        {this.renderTopPanel(`Add a Model to '${name}'`)}
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
    if (!this.state.workspaceId) {
      return this.renderConnections();
    }

    let {algorithm} = createExperimentStore.getState().model_create;
    if (this.state.workspaceId && !this.state.modelId) {
      return this.renderDataPrep();
    }

    if (this.state.modelId && !this.state.isSplitFinalized) {
      return this.renderSplitDataStep();
    }
    if (this.state.isSplitFinalized && !algorithm.length) {
      return this.renderAlgorithmSelectionStep();
    }

    return null;
  }
  render() {
    return (
      <div className="experiments-create-view">
        <Helmet title="CDAP | Create Experiment" />
        {this.renderSteps()}
        <Prompt
          when={!this.state.experimentId || !this.state.modelId}
          message={"Are you sure you want to navigate away?"}
        />
      </div>
    );
  }
}
