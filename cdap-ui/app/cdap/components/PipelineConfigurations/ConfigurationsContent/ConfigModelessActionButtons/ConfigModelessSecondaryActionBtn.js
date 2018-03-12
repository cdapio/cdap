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
    validToSave: state.validToSave,
    pipelineEdited: state.pipelineEdited,
    updatingPipeline: ownProps.updatingPipeline,
    saveAndCloseModeless: ownProps.saveAndCloseModeless,
    isHistoricalRun: ownProps.isHistoricalRun
  };
};

const ConfigModelessSecondaryActionBtn = ({validToSave, pipelineEdited, updatingPipeline, saveAndCloseModeless, isHistoricalRun}) => {
  if (isHistoricalRun) {
    return null;
  }
  return (
    <button
      className="btn btn-secondary"
      disabled={updatingPipeline || !validToSave}
      onClick={saveAndCloseModeless.bind(this, pipelineEdited)}
    >
      {
        updatingPipeline ?
          (
            <span>
              Saving
              <IconSVG
                name="icon-spinner"
                className="fa-spin"
              />
             </span>
          )
        :
          <span>Save</span>
      }
    </button>
  );
};

ConfigModelessSecondaryActionBtn.propTypes = {
  validToSave: PropTypes.bool,
  pipelineEdited: PropTypes.bool,
  updatingPipeline: PropTypes.bool,
  saveAndCloseModeless: PropTypes.func,
  isHistoricalRun: PropTypes.bool
};

const ConnectedConfigModelessSecondaryActionBtn = connect(mapStateToProps)(ConfigModelessSecondaryActionBtn);
export default ConnectedConfigModelessSecondaryActionBtn;
