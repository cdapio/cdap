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

import Button from '@material-ui/core/Button';
import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import Tooltip from '@material-ui/core/Tooltip';
import GetAppIcon from '@material-ui/icons/GetApp';
import * as React from 'react';

const styles = (theme): StyleRules => {
  return {
    buttonTooltip: {
      fontSize: '14px',
      backgroundColor: theme.palette.grey[500],
    },
  };
};

const JSONDownloadButtonView = ({ classes, downloadDisabled, onDownloadClick }) => {
  return (
    <Tooltip
      title={
        downloadDisabled
          ? 'Download is disabled until the required fields are filled in'
          : 'Download Plugin JSON'
      }
      classes={{
        tooltip: classes.buttonTooltip,
      }}
    >
      <div>
        <Button disabled={downloadDisabled} onClick={onDownloadClick}>
          <GetAppIcon />
        </Button>
      </div>
    </Tooltip>
  );
};

const JSONDownloadButton = withStyles(styles)(JSONDownloadButtonView);
export default JSONDownloadButton;
