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
import {connect} from 'react-redux';
import SelectWithOptions from 'components/SelectWithOptions';
import {DashboardActions, ViewByOptions} from 'components/OpsDashboard/store/DashboardStore';
import T from 'i18n-react';

const PREFIX = 'features.OpsDashboard.RunsGraph.Legends.ViewBy';

const options = Object.keys(ViewByOptions).map(option => {
  return {
    id: ViewByOptions[option],
    value: T.translate(`${PREFIX}.${option}`)
  };
});

function ViewByOptionSelectorComp({viewByOption, changeViewByOption}) {
  return (
    <div className="reports-view-by-selector">
      <span>{T.translate(`${PREFIX}.label`)}</span>

      <div className="view-by-selector-dropdown">
        <SelectWithOptions
          options={options}
          value={viewByOption}
          onChange={changeViewByOption}
          className="form-control"
        />
      </div>
    </div>
  );
}

ViewByOptionSelectorComp.propTypes = {
  viewByOption: PropTypes.string,
  changeViewByOption: PropTypes.func
};

const mapStateToProps = (state) => {
  return {
    viewByOption: state.dashboard.viewByOption
  };
};

const mapDispatch = (dispatch) => {
  return {
    changeViewByOption: (e) => {
      dispatch({
        type: DashboardActions.changeViewByOption,
        payload: { viewByOption: e.target.value }
      });
    }
  };
};

const ViewByOptionSelector = connect(
  mapStateToProps,
  mapDispatch
)(ViewByOptionSelectorComp);

export default ViewByOptionSelector;
