/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {PropTypes} from 'react';
import Dropzone from 'react-dropzone';
require('./FileDnD.less');

export default function FileDnD({file, onDropHandler, error}) {
  return (
    <Dropzone
      activeClassName="file-drag-container"
      className="file-drop-container"
      onDrop={onDropHandler}>
      <div className="file-metadata-container text-center">
        <i className="fa fa-upload fa-3x"></i>
        {
          file.name && file.name.length ? (<span>{file.name}</span>)
            :
            (
              <span>
                 Drag and Drop the file to be uploaded
                <br />
                or
                <br />
                Click to select file from your computer
              </span>
            )
        }
        {
          error ?
            <div className="text-danger">
              {error}
            </div>
          :
            null
        }
      </div>
    </Dropzone>
  );
}
FileDnD.propTypes = {
  file: PropTypes.any,
  error: PropTypes.any,
  onDropHandler: PropTypes.func
};
