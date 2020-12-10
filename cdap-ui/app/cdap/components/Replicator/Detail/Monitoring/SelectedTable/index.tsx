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

import React, { useContext } from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { DetailContext } from 'components/Replicator/Detail';
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '0 15px',
      marginBottom: '25px',
      display: 'flex',
      alignItems: 'center',
    },
    reset: {
      color: theme.palette.blue[100],
      cursor: 'pointer',
      marginLeft: '5px',

      '&:hover': {
        textDecoration: 'underline',
      },
    },
    activeTableText: {
      marginLeft: '10px',
    },
    heading: {
      marginBottom: 0,
    },
  };
};

const SelectedTableView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const { activeTable, setActiveTable } = useContext(DetailContext);

  function handleReset() {
    setActiveTable(null);
  }

  const heading = activeTable ? `"${activeTable}"` : 'All tables replicated';

  return (
    <div className={classes.root}>
      <Heading type={HeadingTypes.h3} label={heading} className={classes.heading} />
      <If condition={!!activeTable}>
        <div className={classes.activeTableText}>
          <span>selected -</span>
          <span onClick={handleReset} className={classes.reset}>
            Reset selection
          </span>
        </div>
      </If>
    </div>
  );
};

const SelectedTable = withStyles(styles)(SelectedTableView);
export default SelectedTable;
