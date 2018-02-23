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
import IconSVG from 'components/IconSVG';
import Popover from 'components/Popover';
import SelectWithOptions from 'components/SelectWithOptions';
import {NUM_EXECUTORS_OPTIONS, ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';

const mapStateToProps = (state) => {
  let numExecutorsKeyName = window.CDAP_CONFIG.isEnterprise ? 'system.spark.spark.executor.instances' : 'system.spark.spark.master';
  return {
    numExecutors: state.properties[numExecutorsKeyName]
  };
};
const mapDispatchToProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PipelineConfigurationsActions.SET_NUM_EXECUTORS,
        payload: { numExecutors: e.target.value }
      });
    }
  };
};

const NumExecutors = ({numExecutors, onChange}) => {
  return (
    <div className="label-with-toggle numExecutors form-group row">
      <span className="toggle-label col-xs-4">Number of Executors</span>
      <div className="col-xs-7">
        <SelectWithOptions
          className="form-control small-dropdown"
          value={numExecutors}
          options={NUM_EXECUTORS_OPTIONS}
          onChange={onChange}
        />
        <Popover
          target={() => <IconSVG name="icon-info-circle" />}
          showOn='Hover'
          placement='right'
        >
          The number of executors to allocate for this pipeline on Apache Yarn.
        </Popover>
      </div>
    </div>
  );
};

NumExecutors.propTypes = {
  numExecutors: PropTypes.string,
  onChange: PropTypes.func
};

const ConnectedNumExecutors = connect(mapStateToProps, mapDispatchToProps)(NumExecutors);

export default ConnectedNumExecutors;
