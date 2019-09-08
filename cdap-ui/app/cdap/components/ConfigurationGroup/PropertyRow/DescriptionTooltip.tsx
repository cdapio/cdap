/*
 * Copyright © 2019 Cask Data, Inc.
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
import Tooltip from '@material-ui/core/Tooltip';
import Help from '@material-ui/icons/Help';

const styles = (theme): StyleRules => {
  return {
    icon: {
      color: theme.palette.grey[200],
    },
  };
};

const StyledTooltip = withStyles(
  (theme): StyleRules => {
    return {
      tooltip: {
        backgroundColor: theme.palette.grey[200],
        color: theme.palette.grey[700],
        fontSize: '12px',
        wordBreak: 'break-word',
      },
    };
  }
)(Tooltip);

interface IDescriptionTooltipProps extends WithStyles<typeof styles> {
  description?: string;
}

const DescriptionTooltipView: React.FC<IDescriptionTooltipProps> = ({ description, classes }) => {
  if (!description) {
    return null;
  }

  return (
    <StyledTooltip title={description} placement="left">
      <Help className={classes.icon} />
    </StyledTooltip>
  );
};

const DescriptionTooltip = withStyles(styles)(DescriptionTooltipView);
export default DescriptionTooltip;
