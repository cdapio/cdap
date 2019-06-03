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

import SchemaStore from 'components/SchemaEditor/SchemaStore';
import SchemaEditor from 'components/SchemaEditor';

export default class Experimental extends Component {
  constructor(props) {
    super(props);

    SchemaStore.subscribe(() => {
      const state = SchemaStore.getState();
      const schema = state.schema;

      this.setState({ parsed: schema });
    });
  }

  state = {
    parsed: '',
  };

  componentWillMount() {
    const schema = {
      name: 'schema',
      type: 'record',
      fields: [
        {
          name: 'timestamp',
          type: [
            {
              type: 'long',
              logicalType: 'timestamp-micros',
            },
            'null',
          ],
        },
        {
          name: 'time',
          type: {
            type: 'long',
            logicalType: 'time-micros',
          },
        },
        {
          name: 'date',
          type: {
            type: 'int',
            logicalType: 'date',
          },
        },
        {
          name: 'haha',
          type: [
            {
              type: 'int',
              logicalType: 'date',
            },
            {
              type: 'long',
              logicalType: 'time-micros',
            },
          ],
        },
      ],
    };

    SchemaStore.dispatch({
      type: 'FIELD_UPDATE',
      payload: {
        schema,
      },
    });
  }

  render() {
    return (
      <div>
        <br />
        <br />
        <div className="row">
          <div className="offset-3 col-6">
            <SchemaEditor />
          </div>
        </div>
        <br />
        <br />
        <div className="row">
          <div className="offset-3 col-6">
            <pre>{JSON.stringify(this.state.parsed, null, 2)}</pre>
          </div>
        </div>
      </div>
    );
  }
}
