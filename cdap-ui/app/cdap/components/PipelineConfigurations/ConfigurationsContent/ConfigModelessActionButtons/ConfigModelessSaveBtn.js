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
    saveLoading: ownProps.saveLoading,
    saveConfig: ownProps.saveConfig
  };
};

const ConfigModelessSaveBtn = ({isMissingKeyValues, pipelineEdited, saveLoading, saveConfig}) => {
  return (
    <button
      className="btn btn-secondary"
      disabled={saveLoading || isMissingKeyValues}
      onClick={saveConfig.bind(this, pipelineEdited)}
    >
      {
        saveLoading ?
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

ConfigModelessSaveBtn.propTypes = {
  isMissingKeyValues: PropTypes.bool,
  pipelineEdited: PropTypes.bool,
  saveLoading: PropTypes.bool,
  saveConfig: PropTypes.func
};

const ConnectedConfigModelessSaveBtn = connect(mapStateToProps)(ConfigModelessSaveBtn);
export default ConnectedConfigModelessSaveBtn;
