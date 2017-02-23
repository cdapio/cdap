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

import React, { Component } from 'react';
import WranglerStore from 'components/Wrangler/store';
import WranglerActions from 'components/Wrangler/store/WranglerActions';
import shortid from 'shortid';
import MyWranglerApi from 'api/wrangler';
import DirectivesTabRow from 'components/Wrangler/WranglerSidePanel/DirectivesTabRow';
import fileDownload from 'react-file-download';

export default class DirectivesTab extends Component {
  constructor(props) {
    super(props);

    let store = WranglerStore.getState().wrangler;

    this.state = {
      deleteHover: null,
      directives: store.directives.map((directive) => {
        let obj = {
          name: directive,
          uniqueId: shortid.generate()
        };
        return obj;
      })
    };

    this.download = this.download.bind(this);

    this.sub = WranglerStore.subscribe(() => {
      let state = WranglerStore.getState().wrangler;

      this.setState({
        directives: state.directives.map((directive) => {
          let obj = {
            name: directive,
            uniqueId: shortid.generate()
          };
          return obj;
        })
      });
    });

  }

  componentWillUnmount() {
    this.sub();
  }

  onMouseEnter(index) {
    this.setState({deleteHover: index});
  }
  onMouseLeave() {
    this.setState({deleteHover: null});
  }

  deleteDirective(index) {
    let state = WranglerStore.getState().wrangler;
    let directives = state.directives;

    let newDirectives = directives.slice(0, index);

    let params = {
      namespace: 'default',
      workspaceId: state.workspaceId,
      limit: 100,
      directive: newDirectives
    };

    MyWranglerApi.execute(params)
      .subscribe((res) => {
        this.setState({
          deleteHover: null
        });

        WranglerStore.dispatch({
          type: WranglerActions.setDirectives,
          payload: {
            data: res.value,
            headers: res.header,
            directives: newDirectives
          }
        });
      }, (err) => {
        // Should not ever come to this.. this is only if backend
        // fails somehow
        console.log('Error deleting directives', err);
      });
  }

  download() {
    let state = WranglerStore.getState().wrangler;
    let workspaceId = state.workspaceId,
        directives = state.directives;

    let data = directives.join('\n'),
        filename = `${workspaceId}-directives.txt`;

    fileDownload(data, filename);
  }

  render() {
    return (
      <div className="directives-tab">

        <div className="directives-tab-header">
          <span>#</span>
          <span>Directives</span>
          <span className="float-xs-right">
            <span
              className="fa fa-download"
              onClick={this.download}
            />
          </span>
        </div>

        <div className="directives-tab-body">
          {
            this.state.directives.map((directive, index) => {
              return (
                <DirectivesTabRow
                  key={directive.uniqueId}
                  rowInfo={directive}
                  rowIndex={index}
                  isInactive={this.state.deleteHover !== null && index >= this.state.deleteHover}
                  handleDelete={this.deleteDirective.bind(this, index)}
                  handleMouseEnter={this.onMouseEnter.bind(this, index)}
                  handleMouseLeave={this.onMouseLeave.bind(this)}
                />
              );
            })
          }
        </div>
      </div>
    );
  }
}
