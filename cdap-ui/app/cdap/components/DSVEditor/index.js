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
import {connect , Provider} from 'react-redux';
import DSVActions from 'components/DSVEditor/DSVActions';
import DSVStore from 'components/DSVEditor/DSVStore';
import DSVRow from 'components/DSVEditor/DSVRow';

require('./DSVEditor.scss');

const mapStateToFieldNameProps = (state, ownProps) => {
  return {
    property: state.DSV.rows[ownProps.index].property
  };
};

const fieldToActionMap = {
  property: DSVActions.setProperty
};

const mapDispatchToFieldNameProps = (dispatch, ownProps) => {
  return {
    removeRow: () => {
      dispatch({
        type: DSVActions.deleteRow,
        payload: {index: ownProps.index}
      });
    },
    addRow: () => {
      dispatch({
        type: DSVActions.addRow,
        payload: {index: ownProps.index}
      });
    },
    onChange: (fieldProp, e) => {
      dispatch({
        type: fieldToActionMap[fieldProp],
        payload: {
          index: ownProps.index,
          [fieldProp]: e.target.value
        }
      });
    }
  };
};

let DSVRowWrapper = connect(
  mapStateToFieldNameProps,
  mapDispatchToFieldNameProps
)(DSVRow);

export default class DSVEditor extends Component {
  constructor(props) {
    super(props);

    this.state = {
      rows: this.props.values
    };

    this.sub = DSVStore.subscribe(() => {
      let rows = DSVStore.getState().DSV.rows;

      this.setState({
        rows
      });

      this.props.onChange(rows);
    });

    DSVStore.dispatch({
      type: DSVActions.onUpdate,
      payload: {
        rows: this.props.values
      }
    });
  }

  componentWillReceiveProps(nextProps) {
    this.setState({
      rows: [...nextProps.values]
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
    DSVStore.dispatch({
      type: DSVActions.onReset
    });
  }

  render() {
    return (
      <div className="dsv-editor-container">
        {
          this.state.rows.map( (row, index) => {
            return (
              <div key={row.uniqueId}>
                <Provider store={DSVStore}>
                  <DSVRowWrapper index={index} />
                </Provider>
              </div>
            );
          })
        }
      </div>
    );
  }
}

DSVEditor.propTypes = {
  values: PropTypes.array,
  onChange: PropTypes.func
};
