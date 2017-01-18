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

import React, { Component, PropTypes } from 'react';
import Dropzone from 'react-dropzone';
import T from 'i18n-react';

require('./FileDataUpload.scss');

export default class FileDataUpload extends Component {
  constructor(props) {
    super(props);

    this.state = {
      textarea: false,
      wranglerInput: '',
      file: ''
    };

    this.onContainerClick = this.onContainerClick.bind(this);
    this.handleTextInput = this.handleTextInput.bind(this);
    this.onTextInputBlur = this.onTextInputBlur.bind(this);
  }

  preventPropagation (e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
  }

  handleTextInput(e) {
    this.props.onTextInput(e.target.value);
    this.setState({wranglerInput: e.target.value});
  }

  onContainerClick() {
    this.setState({textarea: true});
  }

  onTextInputBlur() {
    if (this.state.wranglerInput) { return; }
    this.setState({textarea: false});
  }

  render() {
    return (
      <div
        className="file-data-upload-container text-xs-center"
        onClick={this.onContainerClick}
      >
        {
          (!this.state.textarea || this.state.file.name) ?
            (<div
              className="file-data-metadata-container"
            >
              <div
                className="file-data-metadata"
              >
                <div
                  className="upload-data"
                  onClick={(e) => this.preventPropagation(e)}
                >
                  <Dropzone
                    className = 'dropzone'
                    onDrop={(e) => {
                        this.setState({file: e[0], textarea: false});
                        this.props.onDataUpload(e[0]);
                      }
                    }
                  >
                    {
                      this.state.file.name && this.state.file.name.length ?
                        null
                      :
                        (
                          <i className="plus-button fa fa-plus-circle"></i>
                        )
                    }
                  </Dropzone>
                </div>

                <div className="helper-text">
                  {
                    this.state.file.name && this.state.file.name.length ?
                    (<h4 className="file-upload-name">{this.state.file.name}</h4>)
                      :
                    (
                      <div>
                        <h4>
                          {T.translate('features.Wrangler.InputScreen.HelperText.click')}
                          <span className="fa fa-plus-circle" />
                          {T.translate('features.Wrangler.InputScreen.HelperText.upload')}
                        </h4>
                        <h5>{T.translate('features.Wrangler.InputScreen.HelperText.or')}</h5>
                        <h4>{T.translate('features.Wrangler.InputScreen.HelperText.paste')}</h4>
                      </div>
                    )
                  }
                </div>
              </div>
            </div>)
          :
            (
              <textarea
                className="form-control"
                onChange={this.handleTextInput}
                autoFocus={true}
                onBlur={this.onTextInputBlur}
              />
            )
        }
      </div>
    );
  }
}

FileDataUpload.propTypes = {
  onDataUpload: PropTypes.func,
  onTextInput: PropTypes.func
};

