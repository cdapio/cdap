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
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import TransfersList from 'components/Transfers/List/TransfersList';
import AddNewTransfer from 'components/Transfers/List/AddNewTransfer';

const styles = (theme) => {
  return {
    topBar: {
      height: '55px',
      backgroundColor: theme.palette.grey[700],
    },
    container: {
      paddingLeft: '50px',
      paddingRight: '50px',
    },
  };
};

const ListView: React.SFC<WithStyles<typeof styles>> = ({ classes }) => {
  return (
    <div>
      <div className={classes.topBar} />
      <div className={classes.container}>
        <AddNewTransfer />
        <TransfersList />
      </div>
    </div>
  );
};

const List = withStyles(styles)(ListView);
export default List;
