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
import {ViewByOptions} from 'components/OpsDashboard/store/DashboardStore';
import ViewByOptionSelector from 'components/OpsDashboard/RunsGraph/Legends/ViewByOptionSelector';
import T from 'i18n-react';

require('./Legends.scss');

const PREFIX = 'features.OpsDashboard.RunsGraph.Legends';

function startMethodLegends() {
  return (
    <div className="start-method-legends">
      <div className="legend">
        <IconSVG
          name="icon-square-full"
          className="manual"
        />
        <span>{T.translate(`${PREFIX}.manuallyStarted`)}</span>
      </div>

      <div className="legend">
        <IconSVG
          name="icon-square-full"
          className="schedule"
        />
        <span>{T.translate(`${PREFIX}.scheduledTriggered`)}</span>
      </div>
    </div>
  );
}

function statusLegends() {
  return (
    <div className="status-legends">
      <div className="legend">
        <IconSVG
          name="icon-square-full"
          className="running"
        />
        <span>{T.translate(`${PREFIX}.running`)}</span>
      </div>

      <div className="legend">
        <IconSVG
          name="icon-square-full"
          className="successful"
        />
        <span>{T.translate(`${PREFIX}.successful`)}</span>
      </div>

      <div className="legend">
        <IconSVG
          name="icon-square-full"
          className="failed"
        />
        <span>{T.translate(`${PREFIX}.failed`)}</span>
      </div>
    </div>
  );
}

function renderLegendsByOption(viewByOption) {
  return (
    <div className="legends-by-option">
      {
        viewByOption === ViewByOptions.startMethod ?
          startMethodLegends()
        :
          statusLegends()
      }
    </div>
  );
}

function renderDelayLegend() {
  return (
    <div className="delay-legend">
      <img
        src="/cdap_assets/img/delay-legend-icon.png"
        className="delay"
      />
      <span>{T.translate(`${PREFIX}.delay`)}</span>
    </div>
  );
}

function LegendsView({viewByOption}) {
  return (
    <div className="legends-and-selector">
      <ViewByOptionSelector />
      {renderLegendsByOption(viewByOption)}
      {renderDelayLegend()}
    </div>
  );
}

LegendsView.propTypes = {
  viewByOption: PropTypes.string
};

const mapStateToProps = (state) => {
  return {
    viewByOption: state.dashboard.viewByOption
  };
};

const Legends = connect(
  mapStateToProps
)(LegendsView);

export default Legends;
