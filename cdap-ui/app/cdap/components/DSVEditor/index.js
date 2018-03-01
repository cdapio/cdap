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
import {connect , Provider} from 'react-redux';
import DSVActions from 'components/DSVEditor/DSVActions';
import {createDSVStore} from 'components/DSVEditor/DSVStore';
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
    let { values, onChange } = props;

    this.state = {
      rows: values
    };

    this.DSVStore = createDSVStore({values});

    this.sub = this.DSVStore.subscribe(() => {
      let rows = this.DSVStore.getState().DSV.rows;
      if (typeof onChange === 'function') {
        onChange(rows);
      }
      this.setState({ rows });
    });

    this.DSVStore.dispatch({
      type: DSVActions.onUpdate,
      payload: {
        rows: values
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
    this.DSVStore.dispatch({
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
                <Provider store={this.DSVStore}>
                  <DSVRowWrapper
                    index={index}
                    placeholder={this.props.placeholder}
                    disabled={this.props.disabled}
                  />
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
  onChange: PropTypes.func,
  placeholder: PropTypes.string,
  disabled: PropTypes.bool
};
