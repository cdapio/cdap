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
import T from 'i18n-react';

const PREFIX = 'features.PipelineConfigurations.ActionButtons';

const mapStateToProps = (state, ownProps) => {
  return {
    isMissingKeyValues: state.isMissingKeyValues,
    pipelineEdited: state.pipelineEdited,
    saveAndRunLoading: ownProps.saveAndRunLoading,
    saveAndRun: ownProps.saveAndRun
  };
};

const ConfigModelessSaveAndRunBtn = ({isMissingKeyValues, pipelineEdited, saveAndRunLoading, saveAndRun}) => {
  return (
    <button
      className="btn btn-primary apply-action"
      disabled={saveAndRunLoading || isMissingKeyValues}
      onClick={saveAndRun.bind(this, pipelineEdited)}
    >
      <span>{T.translate(`${PREFIX}.saveAndRun`)}</span>
      {
        saveAndRunLoading ?
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

ConfigModelessSaveAndRunBtn.propTypes = {
  isMissingKeyValues: PropTypes.bool,
  pipelineEdited: PropTypes.bool,
  saveAndRunLoading: PropTypes.bool,
  saveAndRun: PropTypes.func,
  actionLabel: PropTypes.string
};

const ConnectedConfigModelessSaveAndRunBtn = connect(mapStateToProps)(ConfigModelessSaveAndRunBtn);
export default ConnectedConfigModelessSaveAndRunBtn;
