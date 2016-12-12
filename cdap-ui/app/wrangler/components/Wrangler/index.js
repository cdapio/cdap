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

import React, { Component, PropTypes } from 'react';
import Papa from 'papaparse';
import WrangleData from 'wrangler/components/Wrangler/WrangleData';
import WranglerActions from 'wrangler/components/Wrangler/Store/WranglerActions';
import WranglerStore from 'wrangler/components/Wrangler/Store/WranglerStore';
import shortid from 'shortid';
import Dropzone from 'react-dropzone';
import {convertHistoryToDml} from 'wrangler/components/Wrangler/dml-converter';

require('./Wrangler.less');

/**
 * 3 Steps for any transforms:
 *    1. Format the data
 *    2. Handle the ordering of Headers
 *    3. Handle the inferred type of the column
 **/
export default class Wrangler extends Component {
  constructor(props) {
    super(props);

    this.state = {
      textarea: false,
      loading: false,
      header: false,
      skipEmptyLines: false,
      delimiter: '',
      wranglerInput: '',
      isDataSet: false,
      file: '',
      errors: []
    };

    this.handleSetHeaders = this.handleSetHeaders.bind(this);
    this.handleSetSkipEmptyLines = this.handleSetSkipEmptyLines.bind(this);
    this.setDelimiter = this.setDelimiter.bind(this);
    this.wrangle = this.wrangle.bind(this);
    this.handleData = this.handleData.bind(this);
    this.handleTextInput = this.handleTextInput.bind(this);
    this.onPlusButtonClick = this.onPlusButtonClick.bind(this);
    this.onWrangleClick = this.onWrangleClick.bind(this);
    this.onWrangleFile = this.onWrangleFile.bind(this);
    this.getWranglerOutput = this.getWranglerOutput.bind(this);
  }

  getChildContext() {
    return {source: this.props.source};
  }

  // componentDidMount() {
  //   this.wrangle();
  // }

  componentWillUnmount() {
    WranglerStore.dispatch({
      type: WranglerActions.reset
    });
  }

  onWrangleClick() {
    this.setState({loading: true});
    this.wrangle();
  }

  onWrangleFile() {
    this.setState({loading: true});
    this.wrangle(true);
  }

  wrangle() {
    let input = this.state.wranglerInput;

    if (this.state.file) {
      input = this.state.file;
    }

    // Keeping these for dev purposes

//     input = `name,num1,num2,num3
// Hakeem Gillespie,91,753,599,a
// Arsenio Gardner,,754,641
// Darius Mcdonald,567,473,520
// Zachary Small,981,271,385
// Travis Rutledge,468,91,578
// Thaddeus Clemons,346,1106,367
// Yardley Merrill,278,1028,473
// Lars Fowler,494,1268,354
// Omar Rocha,856,694,318
// Ryan Chapman,553,1009,565
// Drew Murray,912,402,272
// Henry Reynolds,734,848,643
// Hu Noel,655,668,599
// Abraham Ellis,914,304,307
// Kenyon Newman,83,1040,606
// James Winters,473,327,569
// Asher Mcclure,548,86,458
// Quamar Watts,393,454,547
// Alfonso Webster,986,848,525
// Darius Sharpe,710,931,581
// Gavin Baldwin,139,302,572
// Lyle Hardin,989,857,612
// Nathan Glenn,807,464,334
// Ethan Figueroa,70,834,276
// Silas Wheeler,827,1353,259
// Isaiah Franklin,290,508,259
// Sean Schneider,655,828,622
// Quinlan Hewitt,417,944,671
// Brody Sharp,245,162,511
// Bruno Whitaker,805,1453,390
// Nolan Combs,70,302,696
// Ray Larsen,538,444,232
// Garth Mckee,919,420,379
// Wesley Rivera,122,476,363
// Louis Thornton,731,1288,356
// Jackson Waller,906,801,239
// Beck Singleton,196,391,559
// Abel Anthony,13,572,459
// Victor Gray,856,826,280
// Vance Colon,349,906,264
// Solomon Herrera,128,1181,370
// Drake Church,258,1048,291
// Rafael Ramsey,475,231,428
// Timon Bowen,481,1090,439
// Eric Dunlap,205,1357,582
// Joel Mcbride,794,1237,423
// Kane Richardson,956,241,597
// Luke Zamora,666,1293,200
// Zeus Herrera,84,577,379
// Jeremy Acosta,668,1039,426
// Boris Jimenez,69,406,232`;

    let papaConfig = {
      header: this.state.header,
      skipEmptyLines: this.state.skipEmptyLines,
      complete: this.handleData
    };

    if (this.state.delimiter) {
      papaConfig.delimiter = this.state.delimiter;
    }

    Papa.parse(input, papaConfig);
  }

  onPlusButtonClick() {
    this.setState({textarea: true});
  }

  handleData(papa) {
    if (papa.errors.length > 0) {
      this.setState({errors: papa.errors, loading: false});
      return;
    }

    let formattedData;
    if (Array.isArray(papa.data[0])) {
      // Get length of the longest column
      let numColumns = papa.data.reduce((prev, curr) => {
        return prev > curr.length ? prev : curr.length;
      }, 0);

      formattedData = papa.data.map((row) => {
        let obj = {};
        for (let i = 0; i < numColumns; i++) {
          let key = `column${i+1}`;
          let value = row[i];

          obj[key] = !value ? '' : value;
        }

        return obj;
      });
    } else {
      formattedData = papa.data;
    }

    WranglerStore.dispatch({
      type: WranglerActions.setData,
      payload: {
        data: formattedData,
      }
    });

    this.setState({
      isDataSet: true,
      loading: false,
      delimiter: papa.meta.delimiter
    });
  }

  setDelimiter(e) {
    this.setState({delimiter: e.target.value});
  }

  handleSetHeaders() {
    this.setState({header: !this.state.header});
  }
  handleSetSkipEmptyLines() {
    this.setState({skipEmptyLines: !this.state.skipEmptyLines});
  }
  handleTextInput(e) {
    this.setState({wranglerInput: e.target.value});
  }

  renderErrors() {
    if (this.state.errors.length === 0) {
      return null;
    }

    return (
      <div className="wrangler-error-container">
        <h4 className="error text-center">Errors:</h4>
        <table className="table table-bordered error-table">
          <thead>
            <tr>
              <th>Row</th>
              <th>Error</th>
            </tr>
          </thead>
          <tbody>
            {
              this.state.errors.map((error) => {
                return (
                  <tr key={shortid.generate()}>
                    <td>{error.row}</td>
                    <td>{error.message}</td>
                  </tr>
                );
              })
            }
          </tbody>
        </table>
      </div>
    );

  }

  preventPropagation (e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
  }

  renderWranglerCopyPaste() {
    if (!this.state.textarea || this.state.file.name) {
      return (
        <div
          className="wrangler-plus-button text-center"
          onClick={this.onPlusButtonClick}
        >
          <div
            className="dropzone-container"
            onClick={(e) => this.preventPropagation(e)}
          >
            <Dropzone
              activeClassName="wrangler-file-drag-container"
              className="wrangler-file-drop-container"
              onDrop={(e) => this.setState({file: e[0], textarea: false})}
            >
              {
                this.state.file.name && this.state.file.name.length ?
                  null
                :
                  (
                    <div className="plus-button">
                      <i className="fa fa-plus-circle"></i>
                    </div>
                  )
              }
            </Dropzone>
          </div>

          <div className="plus-button-helper-text">
            {
              this.state.file.name && this.state.file.name.length ?
              (<h4>{this.state.file.name}</h4>)
                :
              (
                <div>
                  <h4>Click <span className="fa fa-plus-circle" /> to upload a file</h4>
                  <h5>or</h5>
                  <h4>Click anywhere else to copy paste data</h4>
                </div>
              )
            }
          </div>
        </div>
      );
    }

    return (
      <textarea
        className="form-control"
        onChange={this.handleTextInput}
        autoFocus={true}
      />
    );
  }

  getWranglerOutput() {
    let state = WranglerStore.getState().wrangler;

    // prepare specifications
    let dml = convertHistoryToDml(state.history.slice(0, state.historyLocation));

    let setFormat = `set format csv ${this.state.delimiter} ${this.state.skipEmptyLines}`;
    let initialColumns = state.initialHeaders.join(',');
    let setColumns = `set columns ${initialColumns}`;

    let spec = [setFormat, setColumns].concat(dml).join('\n');


    // prepare schema
    let fields = [];

    state.headersList.forEach((column) => {
      let hasError = state.errors[column] && state.errors[column].count > 0;

      fields.push({
        name: column,
        type: hasError ? [state.columnTypes[column], 'null'] : state.columnTypes[column]
      });
    });

    let schema = {
      name: 'etlSchemaBody',
      type: 'record',
      fields
    };

    let properties = {
      specification: spec,
      schema: JSON.stringify(schema)
    };

    if (typeof this.props.hydrator === 'function') {
      this.props.hydrator(properties);
    }

    return properties;
  }

  renderWranglerInputBox() {
    if (this.state.isDataSet) {
      return null;
    }

    if (this.state.loading) {
      return (
        <div className="loading text-center">
          <div>
            <span className="fa fa-spinner fa-spin"></span>
          </div>
          <h3>Parsing...</h3>
        </div>
      );
    }

    return (
      <div>
        <div className="wrangler-copy-paste">

          {this.renderWranglerCopyPaste()}

          <div className="parse-options">
            <form className="form-inline">
              <div className="delimiter">
                {/* delimiter */}
                <input
                  type="text"
                  className="form-control"
                  placeholder="Set delimiter"
                  onChange={this.setDelimiter}
                />
              </div>

              <hr/>

              <div className="checkbox">
                {/* header */}
                <label>
                  <input type="checkbox"
                    onChange={this.handleSetHeaders}
                    checked={this.state.headers}
                  /> First line as column name
                </label>
              </div>

              <div className="checkbox">
                {/* skipEmptyLines */}
                <label>
                  <input type="checkbox"
                    onChange={this.handleSetSkipEmptyLines}
                  /> Skip empty lines
                </label>
              </div>
            </form>
          </div>

        </div>

        <br/>

        <div className="text-center">
          <button
            className="btn btn-primary"
            onClick={this.onWrangleClick}
          >
            Wrangle
          </button>
        </div>

        {this.renderErrors()}

      </div>
    );
  }

  render() {
    return (
      <div className="wrangler-container">
        {this.renderWranglerInputBox()}

        {
          this.state.isDataSet ?
            <WrangleData
              onHydratorApply={this.getWranglerOutput}
            />
          :
            null
        }

      </div>
    );
  }
}

Wrangler.propTypes = {
  source: PropTypes.oneOf(['wrangler', 'hydrator']),
  hydrator: PropTypes.any
};

Wrangler.childContextTypes = {
  source: PropTypes.oneOf(['wrangler', 'hydrator'])
};
