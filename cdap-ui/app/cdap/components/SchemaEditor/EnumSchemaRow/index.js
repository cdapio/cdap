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

import React, {PropTypes, Component} from 'react';
import {parseType, checkParsedTypeForError} from 'components/SchemaEditor/SchemaHelpers';
import {Input} from 'reactstrap';
import {insertAt, removeAt} from 'services/helpers';
import uuid from 'node-uuid';
import T from 'i18n-react';
require('./EnumSchemaRow.less');

export default class EnumSchemaRow extends Component {
  constructor(props) {
    super(props);
    if (typeof props.row === 'object') {
      let rowType = parseType(props.row);
      let symbols = rowType.type.getSymbols().map(symbol => ({symbol, id: uuid.v4()}));
      this.state = {
        symbols,
        error: '',
        refRequired: true
      };
    } else {
      this.state = {
        symbols: [{symbol: '', id: uuid.v4()}],
        error: ''
      };
    }
  }
  checkForErrors(symbols) {
    let parsedType = {
      type: 'enum',
      symbols: symbols.map(sobj => sobj.symbol).filter(symbol => symbol.length)
    };
    return checkParsedTypeForError(parsedType);
  }
  onSymbolChange(index, e) {
    let symbols = this.state.symbols;
    symbols[index].symbol = e.target.value;
    let error = this.checkForErrors(symbols);
    if (error) {
      this.setState({error});
      return;
    }
    this.setState({
      symbols,
      error: ''
    }, () => {
      this.props.onChange({
        type: 'enum',
        symbols: this.state.symbols.map(sobj => sobj.symbol).filter(symbol => symbol.length)
      });
    });
  }
  onSymbolAdd(index) {
    let symbols = this.state.symbols.map(sobj => {
      delete sobj.refRequired;
      return sobj;
    });
    symbols = insertAt(symbols, index, {
      symbol: '',
      id: uuid.v4(),
      refRequired: true
    });
    this.setState({symbols}, () => {
      if (this.inputToFocus) {
        this.inputToFocus.focus();
      }
      let error = this.checkForErrors(symbols);
      if (error) {
        return;
      }
      this.props.onChange({
        type: 'enum',
        symbols: this.state.symbols.map(sobj => sobj.symbol).filter(symbol => symbol.length)
      });
    });
  }
  onSymbolRemove(index) {
    let symbols = this.state.symbols;
    symbols = removeAt(symbols, index);
    this.setState({
      symbols,
      error: ''
    }, () => {
      let error = this.checkForErrors(this.state.symbols);
      if (error) {
        this.setState({error});
        return;
      }
      this.props.onChange({
        type: 'enum',
        symbols: this.state.symbols.map(sobj => sobj.symbol).filter(symbol => symbol.length)
      });
    });
  }
  onKeyPress(index, e) {
    if (e.nativeEvent.keyCode === 13) {
      this.onSymbolChange(index, e);
      this.onSymbolAdd(index);
    }
  }
  render() {
    return (
      <div className="enum-schema-row">
        <div className="text-danger">
          {this.state.error}
        </div>
        {
          this.state.symbols.map((sobj, index) => {
            return (
              <div
                className="schema-row"
                key={sobj.id}
              >
                {
                  sobj.refRequired ?
                    <Input
                      className="field-name"
                      placeholder={T.translate('features.SchemaEditor.Labels.symbolName')}
                      defaultValue={sobj.symbol}
                      onFocus={() => sobj.symbol}
                      onBlur={this.onSymbolChange.bind(this, index)}
                      getRef={(ref) => this.inputToFocus = ref}
                      onKeyPress={this.onKeyPress.bind(this, index)}
                    />
                  :
                    <Input
                      className="field-name"
                      placeholder={T.translate('features.SchemaEditor.Labels.symbolName')}
                      defaultValue={sobj.symbol}
                      onFocus={() => sobj.symbol}
                      onBlur={this.onSymbolChange.bind(this, index)}
                      onKeyPress={this.onKeyPress.bind(this, index)}
                    />
                }
                <div className="field-type"></div>
                <div className="field-isnull">
                  <div className="btn btn-link"></div>
                  <div className="btn btn-link">
                    <button
                      className="fa fa-plus"
                      onClick={this.onSymbolAdd.bind(this, index)}
                    ></button>
                  </div>
                  <div className="btn btn-link">
                    {
                      this.state.symbols.length !== 1 ?
                        <button
                          className="fa fa-trash text-danger"
                          onClick={this.onSymbolRemove.bind(this, index)}
                        ></button>
                      :
                        null
                    }
                  </div>
                </div>
              </div>
            );
          })
        }
      </div>
    );
  }
}

EnumSchemaRow.propTypes = {
  row: PropTypes.any,
  onChange: PropTypes.func.isRequired
};
