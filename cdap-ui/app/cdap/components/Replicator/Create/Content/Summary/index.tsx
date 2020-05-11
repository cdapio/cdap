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
import { createContextConnect, ICreateContext } from 'components/Replicator/Create';
import ConfigDisplay from 'components/Replicator/ConfigDisplay';
import TableList from 'components/Replicator/Create/Content/Summary/TableList';
import Heading, { HeadingTypes } from 'components/Heading';
import ActionButtons from 'components/Replicator/Create/Content/Summary/ActionButtons';

const styles = (theme): StyleRules => {
  const borderBottom = `2px solid ${theme.palette.grey[300]}`;

  return {
    root: {
      padding: '15px 40px',
    },
    header: {
      borderBottom,
      paddingBottom: '15px',
    },
    description: {
      color: theme.palette.grey[100],
    },
    configContainer: {
      borderBottom,
    },
  };
};

const SummaryView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  sourcePluginInfo,
  targetPluginInfo,
  sourcePluginWidget,
  targetPluginWidget,
  sourceConfig,
  targetConfig,
  name,
  description,
}) => {
  return (
    <div className={classes.root}>
      <div className={classes.header}>
        <Heading type={HeadingTypes.h4} label={name} />
        <div className={classes.description}>{description}</div>
      </div>
      <div className={classes.configContainer}>
        <ConfigDisplay
          sourcePluginInfo={sourcePluginInfo}
          targetPluginInfo={targetPluginInfo}
          sourcePluginWidget={sourcePluginWidget}
          targetPluginWidget={targetPluginWidget}
          sourceConfig={sourceConfig}
          targetConfig={targetConfig}
        />
      </div>

      <div>
        <TableList />
      </div>

      <ActionButtons />
    </div>
  );
};

const StyledSummary = withStyles(styles)(SummaryView);
const Summary = createContextConnect(StyledSummary);
export default Summary;
