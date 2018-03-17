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

function LegendsView({onClick, manual, schedule, running, success, failed, delay}) {
  return (
    <div className="legends">
      <div className="start-method-legend">
        <div
          className="select-item"
          onClick={onClick.bind(null, 'manual')}
        >
          <IconSVG name={manual ? 'icon-check-square' : 'icon-square-o'} />
          <IconSVG
            name="icon-circle"
            className="manual"
          />
          <span>Manually started runs</span>
        </div>

        <div
          className="select-item"
          onClick={onClick.bind(null, 'schedule')}
        >
          <IconSVG name={schedule ? 'icon-check-square' : 'icon-square-o'} />
          <IconSVG
            name="icon-circle"
            className="schedule"
          />
          <span>Scheduled/triggered runs</span>
        </div>
      </div>

      <div className="status-legend">
        <div
          className="select-item"
          onClick={onClick.bind(null, 'running')}
        >
          <IconSVG name={running ? 'icon-check-square' : 'icon-square-o'} />
          <IconSVG
            name="icon-circle"
            className="running"
          />
          <span>Running</span>
        </div>

        <div
          className="select-item"
          onClick={onClick.bind(null, 'success')}
        >
          <IconSVG name={success ? 'icon-check-square' : 'icon-square-o'} />
          <IconSVG
            name="icon-circle"
            className="successful"
          />
          <span>Successful runs</span>
        </div>

        <div
          className="select-item"
          onClick={onClick.bind(null, 'failed')}
        >
          <IconSVG name={failed ? 'icon-check-square' : 'icon-square-o'} />
          <IconSVG
            name="icon-circle"
            className="failed"
          />
          <span>Failed runs</span>
        </div>
      </div>

      <div
        className="delay-legend"
        onClick={onClick.bind(null, 'delay')}
      >
        <div className="select-item">
          <IconSVG name={delay ? 'icon-check-square' : 'icon-square-o'} />
          <IconSVG
            name="icon-circle"
            className="delay"
          />
          <span>Delay between starting and running</span>
        </div>
      </div>
    </div>
  );
}

LegendsView.propTypes = {
  onClick: PropTypes.func,
  manual: PropTypes.bool,
  schedule: PropTypes.bool,
  running: PropTypes.bool,
  success: PropTypes.bool,
  failed: PropTypes.bool,
  delay: PropTypes.bool
};

const mapStateToProps = (state) => {
  return {
    manual: state.legends.manual,
    schedule: state.legends.schedule,
    running: state.legends.running,
    success: state.legends.success,
    failed: state.legends.failed,
    delay: state.legends.delay
  };
};

const mapDispatch = (dispatch) => {
  return {
    onClick: (type) => {
      dispatch({
        type: DashboardActions.toggleLegend,
        payload: { type }
      });
    }
  };
};

const Legends = connect(
  mapStateToProps,
  mapDispatch
)(LegendsView);

export default Legends;
