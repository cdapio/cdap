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
import { detailContextConnect, IDetailContext } from 'components/Replicator/Detail';
import IconSVG from 'components/IconSVG';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { PROGRAM_INFO } from 'components/Replicator/constants';
import { getLogViewerPageUrl } from 'components/LogViewer/LogViewerPage';

const styles = (theme): StyleRules => {
  return {
    root: {
      textAlign: 'center',
      cursor: 'pointer',
      borderRadius: '4px',
      width: '50px',
      color: theme.palette.grey[50],
      '&:hover': {
        backgroundColor: theme.palette.white[50],
        border: `1px solid ${theme.palette.grey[400]}`,
        color: 'inherit',
        textDecoration: 'none',
      },
    },
    icon: {
      fontSize: '18px',
    },
  };
};

const LogsButtonView: React.FC<IDetailContext & WithStyles<typeof styles>> = ({
  classes,
  runId,
  name,
}) => {
  const namespace = getCurrentNamespace();
  const programType = PROGRAM_INFO.programType;
  const programId = PROGRAM_INFO.programId;

  const logViewerPath = getLogViewerPageUrl(namespace, name, programType, programId, runId);

  return (
    <a className={classes.root} href={logViewerPath} target="_blank" rel="noopener noreferrer">
      <div className={classes.icon}>
        <IconSVG name="icon-file-text-o" />
      </div>
      <div className={classes.text}>Logs</div>
    </a>
  );
};

const StyledLogsButton = withStyles(styles)(LogsButtonView);
const LogsButton = detailContextConnect(StyledLogsButton);
export default LogsButton;
