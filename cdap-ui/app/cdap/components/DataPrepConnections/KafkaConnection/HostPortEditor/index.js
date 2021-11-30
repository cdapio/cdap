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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import { connect, Provider } from 'react-redux';
import HostPortActions from 'components/DataPrepConnections/KafkaConnection/HostPortEditor/HostPortActions';
import HostPortStore from 'components/DataPrepConnections/KafkaConnection/HostPortEditor/HostPortStore';
import HostPortRow from 'components/DataPrepConnections/KafkaConnection/HostPortEditor/HostPortRow';

require('./HostPortEditor.scss');

const mapStateToFieldNameProps = (state, ownProps) => {
  const row = state.hostport.rows[ownProps.index];
  return {
    host: row ? row.host : '',
    port: row ? row.port : '',
  };
};

const fieldToActionMap = {
  host: HostPortActions.setHost,
  port: HostPortActions.setPort,
};

const mapDispatchToFieldNameProps = (dispatch, ownProps) => {
  return {
    removeRow: () => {
      dispatch({
        type: HostPortActions.deletePair,
        payload: { index: ownProps.index },
      });
    },
    addRow: () => {
      dispatch({
        type: HostPortActions.addRow,
        payload: { index: ownProps.index },
      });
    },
    onChange: (fieldProp, e) => {
      dispatch({
        type: fieldToActionMap[fieldProp],
        payload: {
          index: ownProps.index,
          [fieldProp]: e.target.value,
        },
      });
    },
  };
};

let HostPortRowWrapper = connect(
  mapStateToFieldNameProps,
  mapDispatchToFieldNameProps
)(HostPortRow);

export default class HostPortEditor extends Component {
  constructor(props) {
    super(props);

    this.state = {
      rows: this.props.values,
    };

    this.sub = HostPortStore.subscribe(() => {
      let rows = HostPortStore.getState().hostport.rows;

      this.setState({
        rows,
      });

      this.props.onChange(rows);
    });

    HostPortStore.dispatch({
      type: HostPortActions.onUpdate,
      payload: {
        rows: this.props.values,
      },
    });
  }

  componentWillReceiveProps(nextProps) {
    this.setState({
      rows: [...nextProps.values],
    });
  }

  shouldComponentUpdate(nextProps) {
    let check = this.state.rows.length !== nextProps.values.length;

    return check;
  }

  componentWillUnmount() {
    if (this.sub) {
      this.sub();
    }
    HostPortStore.dispatch({
      type: HostPortActions.onReset,
    });
  }

  render() {
    return (
      <div className="host-port-editor-container">
        {this.state.rows.map((row, index) => {
          return (
            <div key={row.uniqueId}>
              <Provider store={HostPortStore}>
                <HostPortRowWrapper index={index} />
              </Provider>
            </div>
          );
        })}
      </div>
    );
  }
}

HostPortEditor.propTypes = {
  values: PropTypes.array,
  onChange: PropTypes.func,
};
