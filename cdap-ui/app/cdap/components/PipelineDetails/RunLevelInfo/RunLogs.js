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
import {connect} from 'react-redux';
import PropTypes from 'prop-types';
import IconSVG from 'components/IconSVG';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {GLOBALS} from 'services/global-constants';
import {objectQuery} from 'services/helpers';
import Popover from 'components/Popover';
import T from 'i18n-react';

const PREFIX = 'features.PipelineDetails.RunLevel';

const mapStateToProps = (state) => {
  return {
    currentRun: state.currentRun,
    runs: state.runs,
    appId: state.name,
    artifactName: state.artifact.name
  };
};

const RunLogs = ({currentRun, runs, appId, artifactName}) => {
  const LogsBtnComp = () => (
    <div className="run-logs-btn">
      <IconSVG name="icon-file-text-o" />
      <div>{T.translate(`${PREFIX}.logs`)}</div>
    </div>
  );

  if (!runs.length) {
    return (
      <Popover
        target={LogsBtnComp}
        showOn='Hover'
        placement='bottom'
        className="run-info-container run-logs-container disabled"
      >
        {T.translate(`${PREFIX}.pipelineNeverRun`)}
      </Popover>
    );
  }

  let namespace = getCurrentNamespace(),
      programType = GLOBALS.programType[artifactName],
      programId = GLOBALS.programId[artifactName],
      runId = objectQuery(currentRun, 'runid');

  let path = `/logviewer/view?namespace=${namespace}&appId=${appId}&programType=${programType}&programId=${programId}&runId=${runId}`;

  return (
    <a href={path} target="_blank">
      <div className="run-info-container run-logs-container">
        <LogsBtnComp />
      </div>
    </a>
  );
};

RunLogs.propTypes = {
  currentRun: PropTypes.object,
  runs: PropTypes.array,
  appId: PropTypes.string,
  artifactName: PropTypes.string
};

const ConnectedRunLogs = connect(mapStateToProps)(RunLogs);
export default ConnectedRunLogs;
