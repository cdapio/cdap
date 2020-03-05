/*
 * Copyright © 2017 Cask Data, Inc.
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

import React from 'react';
import { Provider, connect } from 'react-redux';
import FileDnD from 'components/FileDnD';
import ImportRulebookStore, {
  IMPORTRULEBOOKACTIONS,
} from 'components/RulesEngineHome/ImportRulebookWizard/ImportRulebookStore';

const mapStateToFileDnDProps = (state) => {
  return {
    file: state.upload.file,
  };
};
const mapDispatchToDndProps = (dispatch) => {
  return {
    onDropHandler: (e) => {
      if (!e[0]) {
        return;
      }
      var reader = new FileReader();
      var filename = e[0].name;
      reader.readAsText(e[0], 'UTF-8');
      reader.onload = function(evt) {
        var data = evt.target.result;
        dispatch({
          type: IMPORTRULEBOOKACTIONS.UPLOADFILE,
          payload: {
            file: {
              contents: data,
              name: filename,
            },
          },
        });
      };
    },
  };
};
const ImportRulebookFile = connect(mapStateToFileDnDProps, mapDispatchToDndProps)(FileDnD);

export default function RulebookUploadStep() {
  return (
    <Provider store={ImportRulebookStore}>
      <div className="upload-step-container">
        <ImportRulebookFile />
      </div>
    </Provider>
  );
}
