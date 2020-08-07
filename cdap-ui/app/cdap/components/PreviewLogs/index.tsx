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
import PropTypes from 'prop-types';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import PreviewDataFetcher from 'components/LogViewer/DataFetcher/PreviewDataFetcher';
import LogViewer from 'components/LogViewer';
import ThemeWrapper from 'components/ThemeWrapper';

// 100vh - navbar height - pipeline top bar height - footer height
const height = 'calc(100vh - 48px - 41px - 54px)';

const styles = (theme): StyleRules => {
  return {
    container: {
      left: 0,
      maxWidth: '100%',
      width: '100%',
      maxHeight: height,
      height,
      backgroundColor: theme.palette.white[50],
    },
  };
};

interface IPreviewLogs extends WithStyles<typeof styles> {
  namespace: string;
  previewId: string;
  stopPoll: boolean;
  onClose: () => void;
}

const PreviewLogsWrapper: React.FC<IPreviewLogs> = ({
  classes,
  namespace,
  previewId,
  stopPoll,
  onClose,
}) => {
  const [dataFetcher] = React.useState(
    new PreviewDataFetcher({
      namespace,
      previewId,
    })
  );

  return (
    <div className={classes.container}>
      <LogViewer dataFetcher={dataFetcher} stopPoll={stopPoll} onClose={onClose} />
    </div>
  );
};

const StyledPreviewLogsWrapper = withStyles(styles)(PreviewLogsWrapper);

function PreviewLogs(props) {
  return (
    <ThemeWrapper>
      <StyledPreviewLogsWrapper {...props} />
    </ThemeWrapper>
  );
}

(PreviewLogs as any).propTypes = {
  namespace: PropTypes.string,
  previewId: PropTypes.string,
  stopPoll: PropTypes.bool,
  onClose: PropTypes.func,
};

export default PreviewLogs;
