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

import React, {Component} from 'react';
import Datasource from '../../services/datasource';
import {Observable} from 'rxjs/Observable';
import {MyNamespaceApi} from '../../api/namespace';

export default class ConnectionExample extends Component {
  constructor(props) {
    super(props);

    this.dataSrc = new Datasource();


    MyNamespaceApi.get({
      namespace: 'default',
      haha: 'yoo',
      peace: 'ful',
      target: ['one', 'two', 'three']
    })
      .subscribe((res) => {
        console.log('res', res);
      });

    this.state = {
      data: [],
      error: '',
      isPolling: true
    };
    let streamsReqObj = {
      _cdapPath: '/namespaces/default/streams',
      interval: 2000
    };
    let datasetReqObj = {
      _cdapPath: '/namespaces/default/data/datasets'
    };

    let streams = this.dataSrc.poll(streamsReqObj)
      .map((response) => {
        return response.map((stream) => {
          return {
            name: stream.name,
            type: 'Stream'
          };
        });
      });
    let datasets = this.dataSrc.poll(datasetReqObj)
      .map((response) => {
        return response.map((dataset) => {
          return {
            name: dataset.name,
            type: 'Dataset'
          };
        });
      });

    let combinedData = Observable.combineLatest(streams, datasets);
    this.combination = combinedData.subscribe(
      (response) => {
        let combined = response[0].concat(response[1]);
        this.setState({data: combined});
      },
      (error) => {
        // This error will get executed if either streams or datasets polling
        // gives an error

        this.setState({error: error});
      });
  }

  addStream() {
    let streamName = this.streamNameInput.value;

    let reqObj = {
      _cdapPath: '/namespaces/default/streams/' + streamName,
      method: 'PUT'
    };
    this.dataSrc.request(reqObj)
      .subscribe(
        () => {
          this.streamNameInput.value = '';
        },
        (err) => {
          this.setState({error: err});
        }
      );
  }

  stopPoll() {
    // To stop the poll, just unsubscribe the subscription
    this.combination.unsubscribe();
    this.setState({isPolling: false});
  }

  render() {
    let error;
    if (this.state.error) {
      error = (
        <div className="text-danger">
          {this.state.error}
        </div>
      );
    }

    return (
      <div>
        <h1> Connection Example</h1>
        <h3> Datasets & Streams <small>in Default Namespace</small></h3>

        {error}
        <div className="row">
          <div className="col-xs-3">
            <input type="text"
              className="form-control"
              placeholder="Stream Name"
              ref={(ref) => this.streamNameInput = ref}
            />
          </div>
          <div className="col-xs-3">
            <button className="btn"
              onClick={this.addStream.bind(this)}
            >
              Add Stream
            </button>
          </div>
        </div>
        <br />
        <ul>
          {this.state.data.map((res, index) => <li key={index}>{res.name} ({res.type})</li>)}
        </ul>

        <button
          className="btn btn-danger"
          onClick={this.stopPoll.bind(this)}
          disabled={!this.state.isPolling}
        >
          Stop Poll
        </button>
        <h4>
          Polling Status:
          <i> {this.state.isPolling ? 'Polling' : 'Stopped'} </i>
        </h4>

      </div>
    );
  }
}
