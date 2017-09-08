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
import DataPrepTable from 'components/DataPrep/DataPrepTable';
import DataPrepCLI from 'components/DataPrep/DataPrepCLI';
import isNil from 'lodash/isNil';
import {createStore, combineReducers} from 'redux';
import {connect} from 'react-redux';
import {defaultAction} from 'services/helpers';
import {Provider} from 'react-redux';

const DEFAULTSTORESTATE = {view: 'data'};
const view = (state = 'data', action = defaultAction) => {
  switch (action.type) {
    case 'SETVIEW':
      return action.payload.view || state;
    default:
      return state;
  }
};

const ViewStore = createStore(
  combineReducers({view}),
  DEFAULTSTORESTATE,
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

function ContentSwitch({onSwitchChange}) {
  return (
    <div className="btn-group">
      <div className="btn btn-secondary" onClick={onSwitchChange.bind(null, 'data')}>
        Data
      </div>
      <div className="btn btn-secondary" onClick={onSwitchChange.bind(null, 'viz')}>
        Visualization
      </div>
    </div>
  );
}
ContentSwitch.propTypes = {
  onSwitchChange: PropTypes.func.isRequired
};

const mapStateToProps = () => {
  return {};
};
const mapDispatchToProps = (dispatch) => {
  return {
    onSwitchChange: (view) => {
      dispatch({
        type: 'SETVIEW',
        payload: {view}
      });
    }
  };
};

const SwitchWrapper = connect(
  mapStateToProps,
  mapDispatchToProps
)(ContentSwitch);
const Switch = () => (
  <Provider store={ViewStore}>
    <SwitchWrapper />
  </Provider>
);


export default class DataPrepContentWrapper extends Component {

  componentDidMount() {
    this.viewStoreSubscription = ViewStore.subscribe(() => {
      let {view} = ViewStore.getState();
      this.onSwitchChange(view);
    });
  }

  componentWillUnmount() {
    if (this.viewStoreSubscription) {
      this.viewStoreSubscription();
    }
  }
  onSwitchChange = (view) => {
    if (isNil(view) || (!isNil(view) && this.state.view === view)) {
      return;
    }
    this.setState({
      view
    });
  }

  state = {
    view: 'data'
  };
  render() {
    if (this.state.view === 'data') {
      return (
        <span>
          <DataPrepTable />
          <DataPrepCLI />
        </span>
      );
    }
    if (this.state.view === 'viz') {
      return (
        <h1> Visualization Coming Soon </h1>
      );
    }
    return null;
  }
}

export {Switch, ViewStore};
