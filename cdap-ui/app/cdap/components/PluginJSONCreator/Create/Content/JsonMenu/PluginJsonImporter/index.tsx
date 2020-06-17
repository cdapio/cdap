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
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import SaveIcon from '@material-ui/icons/Save';
import { JSONStatusMessage } from 'components/PluginJSONCreator/Create/Content/JsonMenu';
import { ICreateContext } from 'components/Replicator/Create';
import React from 'react';

const styles = (): StyleRules => {
  return {
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
}) => {
  function processFileUpload() {
    return (e) => {
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
      }
    };
  }

  function renderFileContent(filename, fileContent) {
    populateImportResults(filename, fileContent);
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
        <Button aria-label="save" component="span" color="primary">
          <SaveIcon />
        </Button>
      </label>
    </div>
  );
};

const PluginJSONImporter = withStyles(styles)(PluginJSONImporterView);
export default PluginJSONImporter;
