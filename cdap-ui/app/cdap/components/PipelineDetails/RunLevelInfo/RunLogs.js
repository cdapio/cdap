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
import { connect } from 'react-redux';
import PropTypes from 'prop-types';
import IconSVG from 'components/IconSVG';
import { objectQuery } from 'services/helpers';
import Popover from 'components/Popover';
import T from 'i18n-react';
import ThemeWrapper from 'components/ThemeWrapper';
import LogsPortal from 'components/PipelineDetails/RunLevelInfo/PipelineLogViewer/LogsPortal';
import PipelineLogViewer from 'components/PipelineDetails/RunLevelInfo/PipelineLogViewer';
import If from 'components/If';
import classnames from 'classnames';

const PREFIX = 'features.PipelineDetails.RunLevel';

const RunLogs = ({ currentRun, runs }) => {
  const [isOpen, setIsOpen] = React.useState(false);

  React.useEffect(() => {
    setIsOpen(false);
  }, [objectQuery(currentRun, 'runid')]);

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
        showOn="Hover"
        placement="bottom-end"
        className="run-info-container run-logs-container disabled"
      >
        {T.translate(`${PREFIX}.pipelineNeverRun`)}
      </Popover>
    );
  }

  function toggleLogs() {
    setIsOpen(!isOpen);
  }

  return (
    <ThemeWrapper>
      <span>
        <div
          className={classnames('run-info-container run-logs-container', { active: isOpen })}
          onClick={toggleLogs}
        >
          <LogsBtnComp />
        </div>
        <LogsPortal>
          <If condition={isOpen}>
            <PipelineLogViewer toggleLogViewer={toggleLogs} />
          </If>
        </LogsPortal>
      </span>
    </ThemeWrapper>
  );
};

RunLogs.propTypes = {
  currentRun: PropTypes.object,
  runs: PropTypes.array,
};

const mapStateToProps = (state) => {
  return {
    currentRun: state.currentRun,
    runs: state.runs,
  };
};

const ConnectedRunLogs = connect(mapStateToProps)(RunLogs);
export default ConnectedRunLogs;
