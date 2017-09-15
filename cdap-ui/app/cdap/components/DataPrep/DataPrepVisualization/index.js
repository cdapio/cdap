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
import {CreateVoyager} from 'cask-datavoyager';
import DataPrepStore from 'components/DataPrep/store';
import {setVisualizationState} from 'components/DataPrep/store/DataPrepActionCreator';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import {updateWorkspaceProperties} from 'components/DataPrep/store/DataPrepActionCreator';
import isEqual from 'lodash/isEqual';
import cloneDeep from 'lodash/cloneDeep';
import debounce from 'lodash/debounce';

require('./DataPrepVisualization.scss');

const PLOT_VEGA_LITE_CONFIG = {
  "config": {
    "mark": {
      "color": "#ff6600"
    }
  }
};

export default class DataPrepVisualization extends Component {

  state = {
    loading: true
  }

  /*
    Today we have a 1s timeout before hide loading animation and show the graphs
    if the user switches tabs before that time then we should not show graphs for
    the previous workspace.
  */
  unmounted = false;

  removeDatasetFromVoyagerState = (state) => {
    return {
      ...state,
      dataset: {
        isLoading: false
      }
    };
  };

  addDatasetToVoyagerState = (state, data) => {
    return {
      ...state,
      config: {
        ...state.config,
        vegaPlotSpec: PLOT_VEGA_LITE_CONFIG
      },
      dataset: {
        isLoading: false,
        data: {
          values: data
        }
      }
    };
  };

  renderVoyager = () => {
    let {properties, data} = DataPrepStore.getState().dataprep;
    let visualization = properties.visualization || {};
    this.voyagerInstance = CreateVoyager(this.containerRef, {
      vegaPlotSpec: PLOT_VEGA_LITE_CONFIG
    }, {
      values: data
    });
    if (Object.keys(visualization).length) {
      this.voyagerInstance.setApplicationState(this.addDatasetToVoyagerState(visualization, data));
    }
    this.voyagerStateSubscription = this.voyagerInstance.onStateChange(debounce((voyagerState) => {
      setVisualizationState(this.removeDatasetFromVoyagerState(voyagerState));
    }, 500));
  };

  updateVizProperties = () => {
    let voyagerState = this.voyagerInstance.getApplicationState();
    setVisualizationState(this.removeDatasetFromVoyagerState(voyagerState));
    updateWorkspaceProperties();
    window.removeEventListener('beforeunload', this.updateVizProperties);
  };

  componentDidMount() {
    window.addEventListener('beforeunload', this.updateVizProperties);
    let localData = DataPrepStore.getState().dataprep.data;
    this.datapreSubscription = DataPrepStore.subscribe(() => {
      let {dataprep} = DataPrepStore.getState();
      let properties = dataprep.properties || {};
      let visualization = properties.visualization || {};
      if (!localData || !isEqual(dataprep.data, localData)) {
        if (!Object.keys(visualization).length) {
          this.voyagerInstance.updateData({
            values: dataprep.data
          });
        } else {
          this.voyagerInstance.setApplicationState(this.addDatasetToVoyagerState(visualization, dataprep.data));
        }
        localData = cloneDeep(dataprep.data);
      }
    });
    // This is a hack solely to give it a smooth transition. Without this there is less feedback
    // when the user clicks on Data Relationship tab.
    setTimeout(() => {
      if (!this.unmounted) {
        this.setState({loading: false}, this.renderVoyager);
      }
    }, 1000);
  }

  componentWillUnmount() {
    this.unmounted = true;
    if (this.datapreSubscription) {
      this.datapreSubscription();
    }
    if (this.voyagerStateSubscription) {
      this.voyagerStateSubscription();
    }
    this.updateVizProperties();
    this.voyagerInstance.onComponentWillUnmount();
  }

  render() {
    return (
      <div
        className="datapre-visualization"
        ref={ref => this.containerRef = ref}
      >

        {
          this.state.loading ?
            <LoadingSVGCentered />
          :
            <div id="dataprep-viz"></div>
        }
      </div>
    );
  }
}
