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
import Divider from '@material-ui/core/Divider';
import Paper from '@material-ui/core/Paper';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import If from 'components/If';
import { SecureKeysPageMode } from 'components/SecureKeys';
import SecureKeyCreate from 'components/SecureKeys/SecureKeyCreate';
import SecureKeyActionButtons from 'components/SecureKeys/SecureKeyList/SecureKeyActionButtons';
import SecureKeySearch from 'components/SecureKeys/SecureKeySearch';
import * as React from 'react';

const styles = (theme): StyleRules => {
  return {
    divider: {
      width: '100vw',
    },
    secureKeysTitle: {
      paddingTop: theme.spacing(1),
    },
    secureKeyManager: {
      display: 'grid',
      alignItems: 'center',
      gridTemplateColumns: 'repeat(7, 1fr)',
    },
    addSecureKeyButton: {
      gridRow: '1',
      gridColumnStart: '1',
    },
    secureKeySearch: {
      gridRow: '1',
      gridColumnStart: '7',
    },
    securityKeyRow: {
      cursor: 'pointer',
      hover: {
        cursor: 'pointer',
      },
    },
    nameCell: {
      width: '30%',
    },
    descriptionCell: {
      width: '40%',
    },
    dataCell: {
      width: '20%',
    },
    actionButtonsCell: {
      width: '10%',
    },
  };
};

interface ISecureKeyListProps extends WithStyles<typeof styles> {
  state: any;
  dispatch: React.Dispatch<any>;
}

const SecureKeyListView: React.FC<ISecureKeyListProps> = ({ classes, state, dispatch }) => {
  const { secureKeys, visibilities } = state;

  const [createDialogOpen, setCreateDialogOpen] = React.useState(false);

  const onSecureKeyClick = (keyIndex) => {
    return () => {
      dispatch({ type: 'SET_PAGE_MODE', pageMode: SecureKeysPageMode.Details });
      dispatch({ type: 'SET_ACTIVE_KEY_INDEX', activeKeyIndex: keyIndex });
    };
  };

  return (
    <div>
      <h1 className={classes.secureKeysTitle}>Secure keys</h1>
      <Divider className={classes.divider} />
      <div className={classes.secureKeyManager}>
        <div className={classes.addSecureKeyButton}>
          <Button
            variant="outlined"
            color="primary"
            size="small"
            onClick={() => setCreateDialogOpen(true)}
          >
            Add Secure Key
          </Button>
        </div>
        <div className={classes.secureKeySearch}>
          <SecureKeySearch state={state} dispatch={dispatch} />
        </div>
      </div>

      <Paper>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Key</TableCell>
              <TableCell>Description</TableCell>
              <TableCell>Data</TableCell>
              <TableCell />
            </TableRow>
          </TableHead>
          <TableBody>
            {secureKeys.map((keyMetadata, keyIndex) => {
              const keyID = keyMetadata.get('name');
              return (
                <TableRow
                  key={keyMetadata.get('name')}
                  hover
                  selected
                  className={classes.securityKeyRow}
                  onClick={onSecureKeyClick(keyIndex)}
                >
                  <TableCell className={classes.nameCell}>{keyID}</TableCell>
                  <TableCell className={classes.descriptionCell}>
                    {keyMetadata.get('description')}
                  </TableCell>
                  <TableCell className={classes.dataCell}>
                    <If condition={!visibilities.get(keyID)}>
                      <input id="password" value="password" disabled type="password"></input>
                    </If>
                    <If condition={visibilities.get(keyID)}>{keyMetadata.get('data')}</If>
                  </TableCell>
                  <TableCell className={classes.actionButtonsCell}>
                    <SecureKeyActionButtons
                      state={state}
                      dispatch={dispatch}
                      keyIndex={keyIndex}
                      keyID={keyID}
                    />
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </Paper>

      <SecureKeyCreate
        state={state}
        dispatch={dispatch}
        open={createDialogOpen}
        handleClose={() => setCreateDialogOpen(false)}
      />
    </div>
  );
};

const SecureKeyList = withStyles(styles)(SecureKeyListView);
export default SecureKeyList;
