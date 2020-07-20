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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import Button from '@material-ui/core/Button';
import Paper from '@material-ui/core/Paper';
import SecureKeyActionButtons from 'components/SecureKeys/SecureKeyList/SecureKeyActionButtons';
import SecureKeyCreate from 'components/SecureKeys/SecureKeyCreate';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';

export const CustomTableCell = withStyles((theme) => ({
  head: {
    backgroundColor: theme.palette.grey['300'],
    color: theme.palette.common.white,
    padding: 10,
    fontSize: 14,
    '&:first-of-type': {
      borderRight: `1px solid ${theme.palette.grey['500']}`,
    },
  },
  body: {
    padding: 10,
    fontSize: 14,
    '&:first-of-type': {
      borderRight: `1px solid ${theme.palette.grey['500']}`,
    },
  },
}))(TableCell);

const styles = (theme): StyleRules => {
  return {
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
    root: {
      width: '100%',
      display: 'inline-block',
      height: 'auto',
      marginTop: theme.spacing(1),
    },
    row: {
      height: 40,
      '&:nth-of-type(odd)': {
        backgroundColor: theme.palette.grey['600'],
      },
      cursor: 'pointer',
      hover: {
        cursor: 'pointer',
      },
    },
    nameCell: {
      width: '30%',
    },
    descriptionCell: {
      width: '60%',
    },
    actionButtonsCell: {
      width: '10%',
    },
  };
};

interface ISecureKeyListProps extends WithStyles<typeof styles> {
  state: any;
  alertSuccess: () => void;
  alertFailure: () => void;
  openDeleteDialog: (index: number) => void;
  openEditDialog: (index: number) => void;
}

const SecureKeyListView: React.FC<ISecureKeyListProps> = ({
  classes,
  state,
  alertSuccess,
  alertFailure,
  openDeleteDialog,
  openEditDialog,
}) => {
  const { secureKeys } = state;

  const [createDialogOpen, setCreateDialogOpen] = React.useState(false);

  return (
    <div>
      <h1 className={classes.secureKeysTitle}>Secure keys</h1>
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
      </div>

      <Paper className={classes.root}>
        <Table>
          <TableHead>
            <TableRow className={classes.row}>
              <CustomTableCell>Key</CustomTableCell>
              <CustomTableCell>Description</CustomTableCell>
              <CustomTableCell></CustomTableCell>
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
                  className={classes.row}
                  onClick={() => openEditDialog(keyIndex)}
                >
                  <CustomTableCell className={classes.nameCell}>{keyID}</CustomTableCell>
                  <CustomTableCell className={classes.descriptionCell}>
                    {keyMetadata.get('description')}
                  </CustomTableCell>
                  <CustomTableCell className={classes.actionButtonsCell}>
                    <SecureKeyActionButtons
                      state={state}
                      openDeleteDialog={openDeleteDialog}
                      keyIndex={keyIndex}
                      keyID={keyID}
                    />
                  </CustomTableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </Paper>

      <SecureKeyCreate
        state={state}
        open={createDialogOpen}
        handleClose={() => setCreateDialogOpen(false)}
        alertSuccess={alertSuccess}
        alertFailure={alertFailure}
      />
    </div>
  );
};

const SecureKeyList = withStyles(styles)(SecureKeyListView);
export default SecureKeyList;
