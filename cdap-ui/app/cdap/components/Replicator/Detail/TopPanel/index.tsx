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
import { Link } from 'react-router-dom';
import { getCurrentNamespace } from 'services/NamespaceStore';
import ActionButtons from 'components/Replicator/Detail/TopPanel/ActionButtons';
import Heading, { HeadingTypes } from 'components/Heading';

const styles = (theme): StyleRules => {
  return {
    root: {
      height: '70px',
      backgroundColor: theme.palette.grey[700],
      display: 'grid',
      gridTemplateColumns: '1fr 1fr 550px',
      alignContent: 'center',
      paddingLeft: '40px',
      paddingRight: '40px',

      '& > div': {
        display: 'flex',
        alignItems: 'center',
      },
    },
    caret: {
      marginRight: '5px',
    },
    divider: {
      marginLeft: '5px',
      marginRight: '10px',
    },
  };
};

const TopPanelView: React.FC<IDetailContext & WithStyles<typeof styles>> = ({
  classes,
  name,
  description,
}) => {
  return (
    <div className={classes.root}>
      <div className={classes.heading}>
        <Link to={`/ns/${getCurrentNamespace()}/replication`}>
          <span className={classes.caret}>&laquo;</span>
          <span>All pipelines</span>
        </Link>
        <span className={classes.divider}>|</span>
        <Heading type={HeadingTypes.h2} label={name} />
      </div>
      <div>{description}</div>
      <ActionButtons />
    </div>
  );
};

const StyledTopPanel = withStyles(styles)(TopPanelView);
const TopPanel = detailContextConnect(StyledTopPanel);
export default TopPanel;
