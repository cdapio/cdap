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

import { COMMON_DELIMITER, COMMON_KV_DELIMITER } from 'components/SecureKeys/constants';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import { Map } from 'immutable';
import { MySecureKeyApi } from 'api/securekey';
import React from 'react';
import TextField from '@material-ui/core/TextField';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import { getCurrentNamespace } from 'services/NamespaceStore';

const styles = (theme): StyleRules => {
  return {
    secureKeyInput: {
      margin: `${theme.Spacing(3)}px ${theme.spacing(1)}px`,
    },
  };
};

interface ISecureKeyCreateProps extends WithStyles<typeof styles> {
  state: any;
  open: boolean;
  handleClose: () => void;
  alertSuccess: () => void;
  alertFailure: () => void;
}

const SecureKeyCreateView: React.FC<ISecureKeyCreateProps> = ({
  classes,
  state,
  open,
  handleClose,
  alertSuccess,
  alertFailure,
}) => {
  const { secureKeys } = state;

  const [localName, setLocalName] = React.useState('');
  const [localDescription, setLocalDescription] = React.useState('');
  const [localData, setLocalData] = React.useState('');
  // 'properties' are in key-value form, keep a state in string form
  const [localPropertiesInString, setLocalPropertiesInString] = React.useState('');

  const onLocalNameChange = (e) => {
    setLocalName(e.target.value);
  };

  const onLocalDescriptionChange = (e) => {
    setLocalDescription(e.target.value);
  };

  const onLocalDataChange = (e) => {
    setLocalData(e.target.value);
  };

  const onLocalPropertiesChange = (keyvalue) => {
    setLocalPropertiesInString(keyvalue);
  };

  const convertLocalPropertiesInString = (keyvalue: string) => {
    let keyvaluePairs = Map({});
    keyvalue.split(COMMON_DELIMITER).forEach((pair) => {
      const [key, value] = pair.split(COMMON_KV_DELIMITER);
      keyvaluePairs = keyvaluePairs.set(key, value);
    });
    return keyvaluePairs;
  };

  const saveSecureKey = () => {
    // Duplicate key name should raise an error
    const keyIDs = secureKeys.map((key) => key.get('name'));
    if (keyIDs.includes(localName)) {
      alertFailure();
      return;
    }

    addSecureKey();
  };

  const addSecureKey = () => {
    const namespace = getCurrentNamespace();

    const params = {
      namespace,
      key: localName,
    };

    const requestBody = {
      description: localDescription,
      data: localData,
      properties: convertLocalPropertiesInString(localPropertiesInString),
    };

    MySecureKeyApi.put(params, requestBody).subscribe(() => {
      setLocalName('');
      setLocalDescription('');
      setLocalData('');
      setLocalPropertiesInString('');
      alertSuccess();
      handleClose();
    });
  };

  return (
    <Dialog open={open} onClose={handleClose}>
      <DialogTitle>Create secure key</DialogTitle>
      <DialogContent>
        <div className={classes.secureKeyInput}>
          <TextField
            required
            variant="outlined"
            label="Name"
            defaultValue={localName}
            onChange={onLocalNameChange}
            fullWidth
            InputProps={{
              className: classes.textField,
            }}
            data-cy="secure-key-name"
          />
        </div>
        <div className={classes.secureKeyInput}>
          <TextField
            required
            variant="outlined"
            label="Description"
            defaultValue={localDescription}
            onChange={onLocalDescriptionChange}
            fullWidth
            InputProps={{
              className: classes.textField,
            }}
            data-cy="secure-key-description"
          />
        </div>
        <div className={classes.secureKeyInput}>
          <TextField
            required
            variant="outlined"
            label="Data"
            type="password"
            value={localData}
            onChange={onLocalDataChange}
            fullWidth
            InputProps={{
              className: classes.textField,
            }}
            data-cy="secure-key-data"
          />
        </div>
        <div className={classes.secureKeyInput}>
          <WidgetWrapper
            widgetProperty={{
              label: 'Properties',
              name: 'Properties',
              'widget-type': 'keyvalue',
              'widget-attributes': {
                delimiter: COMMON_DELIMITER,
                'kv-delimiter': COMMON_KV_DELIMITER,
                'key-placeholder': 'key',
                'value-placeholder': 'value',
              },
            }}
            pluginProperty={{
              required: false,
              name: 'Properties',
            }}
            value={localPropertiesInString}
            onChange={(keyvalue) => onLocalPropertiesChange(keyvalue)}
            data-cy="secure-key-properties"
          />
        </div>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} color="primary" data-cy="cancel">
          Cancel
        </Button>
        <Button
          onClick={saveSecureKey}
          color="primary"
          disabled={!localName || !localDescription || !localData}
          data-cy="save-secure-key"
        >
          Save
        </Button>
      </DialogActions>
    </Dialog>
  );
};

const SecureKeyCreate = withStyles(styles)(SecureKeyCreateView);
export default SecureKeyCreate;
