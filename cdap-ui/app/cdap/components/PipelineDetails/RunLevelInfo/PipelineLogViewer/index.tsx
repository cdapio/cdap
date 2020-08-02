/*
 * Copyright © 2020 Cask Data, Inc.
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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { connect } from 'react-redux';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { GLOBALS } from 'services/global-constants';
import ProgramDataFetcher from 'components/LogViewer/DataFetcher/ProgramDataFetcher';
import LogViewer from 'components/LogViewer';
import { PIPELINE_LOGS_FILTER } from 'services/global-constants';

const PIPELINE_TOP_PANEL_OFFSET = '160px';
const FOOTER_HEIGHT = '54px';

const styles = (theme): StyleRules => {
  return {
    portalContainer: {
      position: 'absolute',
      top: 0,
      left: 0,
      height: '100vh',
      width: '100vw',
      zIndex: 1301,
    },
    logsContainer: {
      position: 'absolute',
      top: PIPELINE_TOP_PANEL_OFFSET,
      height: `calc(100% - ${PIPELINE_TOP_PANEL_OFFSET} - ${FOOTER_HEIGHT})`,
      width: '100%',
      backgroundColor: theme.palette.white[50],
    },
  };
};

interface ILogViewerProps extends WithStyles<typeof styles> {
  currentRun: {
    runid: string;
  };
  appId: string;
  artifactName: string;
  toggleLogViewer: () => void;
}

const LogViewerContainer: React.FC<ILogViewerProps> = ({
  classes,
  currentRun,
  appId,
  artifactName,
  toggleLogViewer,
}) => {
  const backgroundElem = React.useRef(null);
  const [dataFetcher] = React.useState(
    new ProgramDataFetcher(
      {
        namespace: getCurrentNamespace(),
        application: appId,
        programType: GLOBALS.programType[artifactName],
        programName: GLOBALS.programId[artifactName],
        runId: currentRun.runid,
      },
      PIPELINE_LOGS_FILTER
    )
  );

  function handleBackgroundClick(e) {
    if (e.target !== backgroundElem.current) {
      return;
    }

    toggleLogViewer();
  }

  return (
    <div className={classes.portalContainer} ref={backgroundElem} onClick={handleBackgroundClick}>
      <div className={classes.logsContainer}>
        <LogViewer dataFetcher={dataFetcher} onClose={toggleLogViewer} />
      </div>
    </div>
  );
};

const StyledLogViewer = withStyles(styles)(LogViewerContainer);

const mapStateToProps = (state) => {
  return {
    currentRun: state.currentRun,
    appId: state.name,
    artifactName: state.artifact.name,
  };
};

const PipelineLogViewer = connect(mapStateToProps)(StyledLogViewer);
export default PipelineLogViewer;
