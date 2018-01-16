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

function ToggleRunsListView({onClick, displayRunsList}) {
  return (
    <div className="toggle-runs-list">
      <span
        className="toggle-span"
        onClick={onClick}
      >
        <IconSVG name={displayRunsList ? 'icon-caret-down' : 'icon-caret-right'} />
        <span>
          {
            displayRunsList ?
              'Hide runs'
            :
              'Show runs'
          }
        </span>
      </span>
    </div>
  );
}

ToggleRunsListView.propTypes = {
  onClick: PropTypes.func,
  displayRunsList: PropTypes.bool
};

const mapStateToProps = (state) => {
  return {
    displayRunsList: state.dashboard.displayRunsList
  };
};

const mapDispatch = (dispatch) => {
  return {
    onClick: () => {
      dispatch({
        type: DashboardActions.toggleDisplayRuns
      });
    }
  };
};


const ToggleRunsList = connect(
  mapStateToProps,
  mapDispatch
)(ToggleRunsListView);

export default ToggleRunsList;
