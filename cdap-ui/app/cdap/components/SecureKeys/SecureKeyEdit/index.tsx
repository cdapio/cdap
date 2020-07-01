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
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import TextField from '@material-ui/core/TextField';
import { MySecureKeyApi } from 'api/securekey';
import classnames from 'classnames';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import { COMMON_DELIMITER, COMMON_KV_DELIMITER } from 'components/PluginJSONCreator/constants';
import { SecureKeyStatus } from 'components/SecureKeys';
import { Map } from 'immutable';
import isNil from 'lodash/isNil';
import React from 'react';
import { getCurrentNamespace } from 'services/NamespaceStore';

const styles = (theme): StyleRules => {
  return {
    margin: {
      margin: `${theme.Spacing(3)}px ${theme.spacing(1)}px`,
    },
    textField: {
      width: '45ch',
    },
    keyvalueField: {
      width: '60ch',
    },
  };
};

interface ISecureKeyEditProps extends WithStyles<typeof styles> {
  keyMetadata: any;
  setSecureKeyStatus: (status: SecureKeyStatus) => void;
  open: boolean;
  handleClose: () => void;
}

const SecureKeyEditView: React.FC<ISecureKeyEditProps> = ({
  classes,
  open,
  handleClose,
  keyMetadata,
  setSecureKeyStatus,
}) => {
  const keyID = keyMetadata ? keyMetadata.get('name') : '';
  const [localDescription, setLocalDescription] = React.useState(
    keyMetadata ? keyMetadata.get('description') : ''
  );
  const [localData, setLocalData] = React.useState(keyMetadata ? keyMetadata.get('data') : '');

  // since 'properties' are in key-value form, keep a separate state in string form
  const properties = keyMetadata ? keyMetadata.get('properties') : '';
  const [localPropertiesInString, setLocalPropertiesInString] = React.useState('');

  const [showData, setShowData] = React.useState(false);
  const [valueIsChanged, setValueIsChanged] = React.useState(false);

  React.useEffect(() => {
    if (isNil(properties)) {
      return;
    }
    setLocalPropertiesInString(
      properties
        .entrySeq()
        .map((e) => e.join(COMMON_KV_DELIMITER))
        .join(COMMON_DELIMITER)
    );
  }, [properties]);

  const onLocalDescriptionChange = (e) => {
    setLocalDescription(e.target.value);
    setValueIsChanged(true);
  };

  const onLocalDataChange = (e) => {
    setLocalData(e.target.value);
    setValueIsChanged(true);
  };

  const onLocalPropertiesChange = (keyvalue) => {
    setLocalPropertiesInString(keyvalue);
    setValueIsChanged(true);
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
    editSecureKey();
  };

  const editSecureKey = () => {
    const namespace = getCurrentNamespace();

    const params = {
      namespace,
      key: keyID,
    };

    const requestBody = {
      description: localDescription,
      data: localData,
      properties: convertLocalPropertiesInString(localPropertiesInString),
    };

    MySecureKeyApi.put(params, requestBody).subscribe(() => {
      setLocalDescription('');
      setLocalData('');
      setLocalPropertiesInString('');
      setSecureKeyStatus(SecureKeyStatus.Success);
      handleClose();
    });
  };

  return (
    <Dialog open={open} onClose={handleClose}>
      <DialogTitle>Edit {keyID}</DialogTitle>
      <DialogContent>
        <div className={classnames(classes.margin, classes.textField)}>
          <TextField
            variant="outlined"
            label="Description"
            defaultValue={localDescription}
            onChange={onLocalDescriptionChange}
            InputProps={{
              className: classes.textField,
            }}
          />
        </div>
        <div className={classnames(classes.margin, classes.textField)}>
          <TextField
            variant="outlined"
            label="Data"
            type={showData ? 'text' : 'password'}
            value={localData}
            onChange={onLocalDataChange}
            InputProps={{
              className: classes.textField,
            }}
          />
        </div>
        <div className={classnames(classes.margin, classes.keyvalueField)}>
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
          />
        </div>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} color="primary">
          Cancel
        </Button>
        <Button onClick={saveSecureKey} color="primary" disabled={!valueIsChanged}>
          Save
        </Button>
      </DialogActions>
    </Dialog>
  );
};

const SecureKeyEdit = withStyles(styles)(SecureKeyEditView);
export default SecureKeyEdit;
