/*
 * Copyright © 2018 Cask Data, Inc.
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

import React from 'react';
import PropTypes from 'prop-types';
import IconSVG from 'components/IconSVG';
import {connect} from 'react-redux';
import {DashboardActions} from 'components/OpsDashboard/store/DashboardStore';

function TypeSelectorView({togglePipeline, toggleCustomApp, pipeline, customApp, pipelineCount, customAppCount}) {
  return (
    <div className="type-selector">
      <div
        className="type-item"
        onClick={togglePipeline}
      >
        <IconSVG name={pipeline ? 'icon-check-square' : 'icon-square-o'} />
        <span>Pipelines ({pipelineCount})</span>
      </div>

      <div
        className="type-item"
        onClick={toggleCustomApp}
      >
        <IconSVG name={customApp ? 'icon-check-square' : 'icon-square-o'} />
        <span>Custom Apps ({customAppCount})</span>
      </div>
    </div>
  );
}

TypeSelectorView.propTypes = {
  togglePipeline: PropTypes.func,
  toggleCustomApp: PropTypes.func,
  pipeline: PropTypes.bool,
  customApp: PropTypes.bool,
  pipelineCount: PropTypes.number,
  customAppCount: PropTypes.number
};

const mapStateToProps = (state) => {
  return {
    pipeline: state.dashboard.pipeline,
    customApp: state.dashboard.customApp,
    pipelineCount: state.dashboard.pipelineCount,
    customAppCount: state.dashboard.customAppCount
  };
};

const mapDispatch = (dispatch) => {
  return {
    togglePipeline: () => {
      dispatch({
        type: DashboardActions.togglePipeline
      });
    },
    toggleCustomApp: () => {
      dispatch({
        type: DashboardActions.toggleCustomApp
      });
    }
  };
};

const TypeSelector = connect(
  mapStateToProps,
  mapDispatch
)(TypeSelectorView);

export default TypeSelector;
