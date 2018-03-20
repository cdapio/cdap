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

const mapStateToProps = (state, ownProps) => {
  return {
    isMissingKeyValues: state.isMissingKeyValues,
    pipelineEdited: state.pipelineEdited,
    saveAndScheduleLoading: ownProps.saveAndScheduleLoading,
    saveAndSchedule: ownProps.saveAndSchedule
  };
};

const ConfigModelessSaveAndScheduleBtn = ({isMissingKeyValues, pipelineEdited, saveAndScheduleLoading, saveAndSchedule}) => {
  return (
    <button
      className="btn btn-primary apply-action"
      disabled={saveAndScheduleLoading || isMissingKeyValues}
      onClick={saveAndSchedule.bind(this, pipelineEdited)}
    >
      <span>Save and Schedule</span>
      {
        saveAndScheduleLoading ?
          <IconSVG
            name="icon-spinner"
            className="fa-spin"
          />
        :
          null
      }
    </button>
  );
};

ConfigModelessSaveAndScheduleBtn.propTypes = {
  isMissingKeyValues: PropTypes.bool,
  pipelineEdited: PropTypes.bool,
  saveAndScheduleLoading: PropTypes.bool,
  saveAndSchedule: PropTypes.func
};

const ConnectedConfigModelessSaveAndScheduleBtn = connect(mapStateToProps)(ConfigModelessSaveAndScheduleBtn);
export default ConnectedConfigModelessSaveAndScheduleBtn;
