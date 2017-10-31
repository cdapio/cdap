/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {Component} from 'react';
import TopPanel from 'components/Experiments/TopPanel';
import IconSVG from 'components/IconSVG';
import DataPrepConnections from 'components/DataPrepConnections';
import DataPrepHome from 'components/DataPrepHome';
import {Prompt} from 'react-router-dom';
import createExperimentStore from 'components/Experiments/store/createExperimentStore';
import MyDataPrepApi from 'api/dataprep';
import NamespaceStore from 'services/NamespaceStore';
import UncontrolledPopover from 'components/UncontrolledComponents/Popover';
import ExperimentPopovers from 'components/Experiments/Popovers';
import DataPrepStore from 'components/DataPrep/store';
import {setOutcomeColumns, setDirectives, setSrcPath, setWorkspace} from 'components/Experiments/store/ActionCreator';
import MLAlgorithmSelection from 'components/Experiments/MLAlgorithmSelection';
import ExperimentMetadata from 'components/Experiments/ExperimentMetadata';
import LoadingSVGCentered from 'components/LoadingSVGCentered';

require('./CreateView.scss');

export default class ExperimentCreateView extends Component {
  state = {
    workspaceId: '',
    isModelCreated: createExperimentStore.getState().model_create.isModelCreated
  };
  componentDidMount() {
    this.dataprepsubscription = DataPrepStore.subscribe(() => {
      let {dataprep} = DataPrepStore.getState();
      let {headers = [], directives, workspaceInfo = {}} = dataprep;
      if (!headers.length) {
        return;
      }
      setSrcPath(workspaceInfo.properties.path);
      setOutcomeColumns(headers);
      setDirectives(directives);
    });
    this.createExperimentStoreSubscription = createExperimentStore.subscribe(() => {
      let {model_create, experiments_create} = createExperimentStore.getState();
      let {isModelCreated} = model_create;
      if (this.state.isModelCreated !== isModelCreated) {
        this.setState({ isModelCreated });
      }
      if (experiments_create.loading) {
        this.setState({
          loading: true
        });
      }
    });
  }
  componentWillUnmount() {
    if (this.dataprepsubscription) {
      this.dataprepsubscription();
    }
    let {isExperimentCreated} = createExperimentStore.getState().experiments_create;
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let workspaceId = this.state.workspaceId;
    if (!isExperimentCreated) {
      MyDataPrepApi
        .delete({
          namespace,
          workspaceId
        }).subscribe();
    }
  }

  renderConnections() {
    return (
      <span>
        <TopPanel>
          <h4>Create a New Experiment</h4>
          <IconSVG name="icon-close" />
        </TopPanel>
        <DataPrepConnections
          sidePanelExpanded={true}
          enableRouting={false}
          singleWorkspaceMode={true}
          onWorkspaceCreate={(workspaceId) => {
            setWorkspace(workspaceId);
            this.setState({workspaceId});
          }}
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
        <TopPanel>
          <h4>Create a New Experiment</h4>
          <IconSVG name="icon-close" />
        </TopPanel>
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
  renderAlgorithmSelectionStep() {
    let {name} = createExperimentStore.getState().experiments_create;
    return (
      <span className="algorithm-selection-step">
        <TopPanel>
          <h4>
            Add a Model to {`'${name}'`}
          </h4>
          <IconSVG name="icon-close" />
        </TopPanel>
        <ExperimentMetadata />
        <hr />
        <MLAlgorithmSelection />
      </span>
    );
  }
  renderSteps() {
    if (!this.state.workspaceId) {
      return this.renderConnections();
    }

    let {algorithm} = createExperimentStore.getState().model_create;
    if (this.state.workspaceId && !this.state.isModelCreated) {
      return this.renderDataPrep();
    }

    if (this.state.isModelCreated && !algorithm.length) {
      return this.renderAlgorithmSelectionStep();
    }

    return null;
  }
  render() {
    return (
      <div className="experiments-create-view">
        {this.renderSteps()}
        {this.state.loading ? <LoadingSVGCentered /> : null}
        <Prompt message={"Are you sure you want to navigate away?"} />
      </div>
    );
  }
}
