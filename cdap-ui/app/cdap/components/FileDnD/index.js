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

import PropTypes from 'prop-types';

import React, { useCallback } from 'react';
import { useDropzone } from 'react-dropzone';
require('./FileDnD.scss');
import classnames from 'classnames';
import T from 'i18n-react';

export default function FileDnD({ file, onDropHandler, error, uploadLabel, clickLabel }) {
  const onDrop = useCallback((acceptedFiles) => {
    onDropHandler(acceptedFiles);
  }, []);
  const { getRootProps, getInputProps, isDragActive } = useDropzone({ onDrop });

  return (
    <div
      {...getRootProps({ 'data-cy': 'file-drop-zone' })}
      className={classnames('file-drop-container', {
        'file-drag-container': isDragActive,
      })}
    >
      <input {...getInputProps()} />
      <div className="file-metadata-container text-center">
        <i className="fa fa-upload fa-3x" />
        {file.name && file.name.length ? (
          <span>{file.name}</span>
        ) : (
          <span>
            {uploadLabel ? uploadLabel : T.translate('features.FileDnD.uploadLabel')}
            <br />
            or
            <br />
            {clickLabel ? clickLabel : T.translate('features.FileDnD.clickLabel')}
          </span>
        )}
        {error ? <div className="text-danger">{error}</div> : null}
      </div>
    </div>
  );
}
FileDnD.propTypes = {
  file: PropTypes.any,
  uploadLabel: PropTypes.string,
  clickLabel: PropTypes.string,
  error: PropTypes.any,
  onDropHandler: PropTypes.func,
};
