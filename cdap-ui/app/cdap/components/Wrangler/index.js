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

import React, { Component } from 'react';
import Papa from 'papaparse';

require('./Wrangler.less');

export default class Wrangler extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div className="wrangler-container">
        <h1>Wrangler</h1>

        <div className="wrangler-input row">
          <div className="col-xs-6">
            <h3>Upload File</h3>

            <input type="file" />
          </div>
          <div className="col-xs-6">
            <h3>Copy Input Text</h3>
            <textarea className="form-control"></textarea>
          </div>
        </div>

        <div className="parse-options">
          <h3>Options</h3>

          <form className="form-inline">
            <div className="delimiter">
              {/* delimiter */}
              <label className="control-label">Delimiter</label>
              <input type="text" className="form-control" />
            </div>

            <div className="checkbox">
              {/* header */}
              <label>
                <input type="checkbox"/> First line as column name?
              </label>
            </div>

            <div className="checkbox">
              {/* skipEmptyLines */}
              <label>
                <input type="checkbox"/> Skip empty lines?
              </label>
            </div>

            <div className="checkbox">
              {/* dynamicTyping */}
              <label>
                <input type="checkbox"/> Attempt to convert number and booleans?
              </label>
            </div>
          </form>
        </div>

        <br/>
        <div className="text-center">
          <button className="btn btn-primary">Wrangle</button>
        </div>
      </div>
    );
  }
}
