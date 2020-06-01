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

import { Button, CircularProgress, WithStyles, withStyles } from '@material-ui/core';
import { green } from '@material-ui/core/colors';
import { StyleRules } from '@material-ui/core/styles';
import CheckIcon from '@material-ui/icons/Check';
import SaveIcon from '@material-ui/icons/Save';
import clsx from 'clsx';
import { JSONStatusMessage } from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import { ICreateContext } from 'components/Replicator/Create';
import React from 'react';

const styles = (theme): StyleRules => {
  return {
    buttonSuccess: {
      backgroundColor: green[500],
      '&:hover': {
        backgroundColor: green[700],
      },
    },
    buttonProgress: {
      color: green[500],
      position: 'absolute',
      top: '50%',
      left: '50%',
      marginTop: -12,
      marginLeft: -12,
    },
    fileInput: {
      display: 'none',
    },
  };
};

interface IPluginJSONImporterProps extends WithStyles<typeof styles>, ICreateContext {
  populateImportResults: (filename: string, fileContent: string) => void;
  JSONStatus: JSONStatusMessage;
}

const PluginJSONImporterView: React.FC<IPluginJSONImporterProps> = ({
  classes,
  populateImportResults,
  JSONStatus,
}) => {
  const [loading, setLoading] = React.useState(false);
  const [success, setSuccess] = React.useState(false);

  const buttonClassname = clsx({
    [classes.buttonSuccess]: success,
  });

  React.useEffect(() => {
    if (JSONStatus === JSONStatusMessage.Success) {
      setSuccess(true);
    } else {
      setSuccess(false);
    }
  }, [JSONStatus]);

  function processFileUpload() {
    return (e) => {
      if (!loading) {
        setLoading(true);
      }
      const files = e.target.files;
      if (files.length > 0) {
        const filename = files[0].name;
        const filenameWithoutExtension =
          filename.substring(0, filename.lastIndexOf('.')) || filename;
        const reader = new FileReader();
        reader.readAsText(files[0]);
        let fileContent;
        reader.onload = (r) => {
          fileContent = r.target.result;
          renderFileContent(filenameWithoutExtension, fileContent);
        };
      } else {
        setLoading(false);
      }
    };
  }

  async function renderFileContent(filename, fileContent) {
    await populateImportResults(filename, fileContent);
    setLoading(false);
  }

  return (
    <div>
      <input
        accept="json/*"
        id="raised-button-file"
        type="file"
        className={classes.fileInput}
        onChange={processFileUpload()}
      />
      <label htmlFor="raised-button-file">
        <Button
          aria-label="save"
          component="span"
          color="primary"
          disabled={loading}
          className={buttonClassname}
        >
          {success ? <CheckIcon /> : <SaveIcon />}
        </Button>
        {loading && <CircularProgress size={24} className={classes.buttonProgress} />}
      </label>
    </div>
  );
};

const PluginJSONImporter = withStyles(styles)(PluginJSONImporterView);
export default PluginJSONImporter;
