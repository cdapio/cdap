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
import FormControl from '@material-ui/core/FormControl';
import IconButton from '@material-ui/core/IconButton';
import InputAdornment from '@material-ui/core/InputAdornment';
import Paper from '@material-ui/core/Paper';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import TextField from '@material-ui/core/TextField';
import ArrowBackIcon from '@material-ui/icons/ArrowBack';
import FileCopyOutlinedIcon from '@material-ui/icons/FileCopyOutlined';
import Visibility from '@material-ui/icons/Visibility';
import VisibilityOff from '@material-ui/icons/VisibilityOff';
import classnames from 'classnames';
import { SecureKeysPageMode } from 'components/SecureKeys';
import { List } from 'immutable';
import * as React from 'react';
import { copyToClipBoard } from 'services/Clipboard';

const styles = (theme): StyleRules => {
  return {
    secureKeysTitle: {
      paddingTop: theme.spacing(1),
    },
    divider: {
      width: '100vw',
    },
    secureKeyDetails: {
      margin: `${theme.Spacing(3)}px 0px`,
      border: `1px solid ${theme.palette.grey[300]}`,
      borderRadius: '6px',
      padding: theme.spacing(3),
      display: 'grid',
      gridTemplateColumns: 'repeat(4, 1fr)',
    },
    keyID: {
      gridColumnStart: '1',
      gridColumnEnd: '2',
    },
    details: {
      gridColumnStart: '2',
      gridColumnEnd: '5',
    },
    secureKeyActionButton: {
      margin: theme.spacing(1),
      float: 'right',
    },
    secureKeyInput: {
      margin: `${theme.Spacing(1)}px 0px`,
      whiteSpace: 'nowrap',
      wordWrap: 'normal',
      paddingRight: '12px',
      letterSpacing: '.00625em',
      fontSize: '1rem',
      lineHeight: '1.5rem',
    },
  };
};

interface ISecureKeysDetailsProps extends WithStyles<typeof styles> {
  secureKeys: List<any>;
  activeKeyIndex: number;
  setActiveKeyIndex: (index: number) => void;
  setPageMode: (pageMode: SecureKeysPageMode) => void;
  deleteKey: (index: number) => void;
  setEditMode: (mode: boolean) => void;
  setDeleteMode: (mode: boolean) => void;
}

const SecureKeysDetailsView: React.FC<ISecureKeysDetailsProps> = ({
  classes,
  activeKeyIndex,
  secureKeys,
  setActiveKeyIndex,
  setPageMode,
  setEditMode,
  setDeleteMode,
}) => {
  const [showData, setShowData] = React.useState(false);

  const keyMetadata = secureKeys.get(activeKeyIndex);

  const handleBackButtonClick = () => {
    setPageMode(SecureKeysPageMode.List);
    setActiveKeyIndex(null);
  };

  return (
    <div>
      <h1 className={classes.secureKeysTitle}>
        <IconButton onClick={handleBackButtonClick}>
          <ArrowBackIcon />
        </IconButton>
        {keyMetadata.get('name')}
      </h1>
      <Divider className={classes.divider} />
      <Paper className={classes.secureKeyDetails} elevation={0}>
        <div className={classes.keyID}>{keyMetadata.get('name')}</div>
        <div className={classes.details}>
          <FormControl
            fullWidth
            className={classnames(classes.margin, classes.textField)}
            variant="outlined"
          >
            <TextField
              type={'text'}
              value={keyMetadata.get('description')}
              variant="filled"
              className={classes.secureKeyInput}
              InputProps={{
                readOnly: true,
                endAdornment: (
                  <InputAdornment position="end">
                    <IconButton onClick={() => copyToClipBoard(keyMetadata.get('description'))}>
                      <FileCopyOutlinedIcon />
                    </IconButton>
                  </InputAdornment>
                ),
              }}
            />
          </FormControl>

          <FormControl
            fullWidth
            className={classnames(classes.margin, classes.textField)}
            variant="outlined"
          >
            <TextField
              type={showData ? 'text' : 'password'}
              value={'password'}
              variant="filled"
              color="primary"
              className={classes.secureKeyInput}
              InputProps={{
                readOnly: true,
                endAdornment: (
                  <InputAdornment position="end">
                    <IconButton
                      aria-label="toggle password visibility"
                      onClick={() => setShowData(!showData)}
                      edge="end"
                    >
                      {showData ? <Visibility /> : <VisibilityOff />}
                    </IconButton>
                    <IconButton onClick={() => copyToClipBoard(keyMetadata.get('data'))}>
                      <FileCopyOutlinedIcon />
                    </IconButton>
                  </InputAdornment>
                ),
              }}
            />
          </FormControl>

          <Button
            className={classes.secureKeyActionButton}
            variant="outlined"
            color="primary"
            size="medium"
            onClick={() => setDeleteMode(true)}
          >
            Delete
          </Button>
          <Button
            className={classes.secureKeyActionButton}
            variant="outlined"
            color="primary"
            size="medium"
            onClick={() => setEditMode(true)}
          >
            Edit
          </Button>
        </div>
      </Paper>
    </div>
  );
};

const SecureKeysDetails = withStyles(styles)(SecureKeysDetailsView);
export default SecureKeysDetails;
