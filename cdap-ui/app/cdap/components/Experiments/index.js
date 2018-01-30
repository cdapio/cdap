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

import React, { Component } from 'react';
import {myExperimentsApi} from 'api/experiments';
import {getCurrentNamespace} from 'services/NamespaceStore';
import Loadable from 'react-loadable';
import {Route, Switch} from 'react-router-dom';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import ExperimentsServiceControl from 'components/Experiments/ExperimentsServiceControl';
import ExperimentsList from 'components/Experiments/ListView';

const ExperimentsCreateView = Loadable({
  loader: () => import(/* webpackChunkName: "ExperimentsCreateView" */ 'components/Experiments/CreateView'),
  loading: LoadingSVGCentered
});

const ExperimentDetailedView = Loadable({
  loader: () => import(/* webpackChunkName: "ExperimentsDetailedView" */ 'components/Experiments/DetailedView'),
  loading: LoadingSVGCentered
});

export default class Experiments extends Component {
  componentWillMount() {
    this.checkIfMMDSRunning();
  }

  state = {
    loading: true,
    isRunning: false
  };

  checkIfMMDSRunning = () => {
    let namespace = getCurrentNamespace();

    myExperimentsApi.list({namespace})
      .subscribe(() => {
        this.setState({
          loading: false,
          isRunning: true
        });
      }, () => {
        this.setState({
          loading: false,
          isRunning: false
        });
      });
  };

  onServiceStart = () => {
    this.setState({
      loading: false,
      isRunning: true
    });
  };

  renderLoading() {
    return (
      <LoadingSVGCentered />
    );
  }

  render() {
    if (this.state.loading) {
      return this.renderLoading();
    }

    if (!this.state.isRunning) {
      return (
        <ExperimentsServiceControl
          onServiceStart={this.onServiceStart}
        />
      );
    }

    return (
      <Switch>
        <Route exact path="/ns/:namespace/experiments" component={ExperimentsList} />
        <Route exact path="/ns/:namespace/experiments/create" component={ExperimentsCreateView} />
        <Route exact path="/ns/:namespace/experiments/:experimentId" component={ExperimentDetailedView} />
      </Switch>
    );
  }
}
