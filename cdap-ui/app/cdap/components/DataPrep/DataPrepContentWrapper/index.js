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
import DataPrepVisualization from 'components/DataPrep/DataPrepVisualization';
import DataPrepSidePanel from 'components/DataPrep/DataPrepSidePanel';
import classnames from 'classnames';
import T from 'i18n-react';
require('./DataPrepContentWrapper.scss');

const PREFIX = 'features.DataPrep.TopPanel';
const DEFAULTVIEW = 'data';
const DEFAULTSTORESTATE = {view: DEFAULTVIEW};
const view = (state = DEFAULTVIEW, action = defaultAction) => {
  switch (action.type) {
    case 'SETVIEW':
      return action.payload.view || state;
    case 'RESET':
      return DEFAULTVIEW;
    default:
      return state;
  }
};

const ViewStore = createStore(
  combineReducers({view}),
  DEFAULTSTORESTATE,
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

function ContentSwitch({onSwitchChange, activeTab}) {
  return (
    <div className="content-switch">
      <div
        className={classnames("switch", {
          'active': activeTab === 'data'
        })}
        onClick={onSwitchChange.bind(null, 'data')}
      >
        {T.translate(`${PREFIX}.Tabs.dataprep`)}
      </div>
      <div
        className={classnames("switch", {
          'active': activeTab === 'viz'
        })}
        onClick={onSwitchChange.bind(null, 'viz')}
      >
      {T.translate(`${PREFIX}.Tabs.dataviz`)}
      </div>
    </div>
  );
}
ContentSwitch.propTypes = {
  onSwitchChange: PropTypes.func.isRequired,
  activeTab: PropTypes.string
};

const mapStateToProps = (state) => {
  return {
    activeTab: state.view
  };
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
      ViewStore.dispatch({
        type: 'RESET'
      });
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
    const dataPart = (
      <div className="row">
        <div className="dataprep-main col-xs-9">
          <DataPrepTable />
          <DataPrepCLI />
        </div>
        <DataPrepSidePanel />
      </div>
    );
    const vizPart = (
      <div className="row">
        <div className="col-xs-12">
          <DataPrepVisualization />
        </div>
      </div>
    );
    let content = null;
    if (this.state.view === 'data') {
      content = dataPart;
    }
    if (this.state.view === 'viz') {
      content = vizPart;
    }
    return (
      <div className="dataprep-content-wrapper">
        {content}
      </div>
    );
  }
}

export {Switch, ViewStore};
