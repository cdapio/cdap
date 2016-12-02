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
import WrangleData from 'components/Wrangler/WrangleData';

import WranglerActions from 'components/Wrangler/Store/WranglerActions';
import WranglerStore from 'components/Wrangler/Store/WranglerStore';

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
      originalData: []
    };

    this.handleSetHeaders = this.handleSetHeaders.bind(this);
    this.handleSetSkipEmptyLines = this.handleSetSkipEmptyLines.bind(this);
    this.setDelimiter = this.setDelimiter.bind(this);
    this.wrangle = this.wrangle.bind(this);
    this.handleData = this.handleData.bind(this);
    this.handleTextInput = this.handleTextInput.bind(this);
    this.onPlusButtonClick = this.onPlusButtonClick.bind(this);
    this.onWrangleClick = this.onWrangleClick.bind(this);
  }

  // componentDidMount() {
  //   this.wrangle();
  // }

  onWrangleClick() {
    this.setState({loading: true});
    this.wrangle();
  }

  wrangle() {
    let input = this.state.wranglerInput;

    // Keeping these for dev purposes

//     let input = `Hakeem Gillespie,91,753,599
// Arsenio Gardner,683,754,641
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
    let formattedData;
    if (Array.isArray(papa.data[0])) {
      formattedData = papa.data.map((row) => {
        let obj = {};

        row.forEach((col, index) => {
          let key = `column${index+1}`;
          obj[key] = col;
        });

        return obj;
      });
    } else {
      formattedData = papa.data;
    }

    WranglerStore.dispatch({
      type: WranglerActions.setData,
      payload: {
        data: formattedData
      }
    });

    this.setState({
      originalData: formattedData,
      loading: false
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

  renderWranglerCopyPaste() {
    if (!this.state.textarea) {
      return (
        <div
          className="wrangler-plus-button text-center"
          onClick={this.onPlusButtonClick}
        >
          <div className="plus-button">
            <i className="fa fa-plus-circle"></i>
          </div>
          <div className="plus-button-helper-text">
            <h4>Copy-paste data here</h4>
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

  renderWranglerInputBox() {
    if (this.state.originalData.length !== 0) {
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
      </div>
    );
  }

  render() {
    return (
      <div className="wrangler-container">
        {this.renderWranglerInputBox()}

        {
          this.state.originalData.length ?
            <WrangleData data={this.state.originalData} />
          :
            null
        }

      </div>
    );
  }
}
