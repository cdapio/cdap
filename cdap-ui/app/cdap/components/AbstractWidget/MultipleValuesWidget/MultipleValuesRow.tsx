/*
 * Copyright © 2019 Cask Data, Inc.
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

import * as React from 'react';
import Input from '@material-ui/core/Input';
import withStyles from '@material-ui/core/styles/withStyles';
import AbstractRow, {
  IAbstractRowProps,
  AbstractRowStyles,
} from 'components/AbstractWidget/AbstractMultiRowWidget/AbstractRow';

const styles = (theme) => {
  return {
    root: {
      ...AbstractRowStyles(theme).root,
      height: 'initial',
      marginBottom: '15px',
    },
    disabled: {
      color: `${theme.palette.grey['50']}`,
    },
  };
};

interface IMultipleValuesRowProps extends IAbstractRowProps<typeof styles> {
  placeholders?: string[];
  valuesDelimiter?: string;
  numValues: number;
}

class MultipleValuesRow extends AbstractRow<IMultipleValuesRowProps, {}> {
  public static defaultProps = {
    valuesDelimiter: ':',
    numValues: 2,
    placeholders: [],
  };

  public state = {};

  public componentDidMount() {
    const split = this.props.value.split(this.props.valuesDelimiter);
    const obj = {};

    for (let i = 0; i < this.props.numValues; i++) {
      obj[i] = split[i] || '';
    }

    this.setState(obj);
  }

  private handleChange = (index, e) => {
    this.setState(
      {
        [index]: e.target.value,
      },
      () => {
        const values = [];
        for (let i = 0; i < this.props.numValues; i++) {
          values[i] = this.state[i];
        }

        this.onChange(values.join(this.props.valuesDelimiter));
      }
    );
  };

  public renderInput = () => {
    const fields = new Array(this.props.numValues).fill('');
    return (
      <div>
        {fields.map((x, i) => {
          const fieldValue = this.state[i] || '';

          return (
            <Input
              key={i}
              data-cy={`multiple-values-input-${i}`}
              classes={{ disabled: this.props.classes.disabled }}
              placeholder={this.props.placeholders[i]}
              onChange={this.handleChange.bind(this, i)}
              value={fieldValue}
              fullWidth={true}
              disabled={this.props.disabled}
              inputRef={i === 0 ? this.props.forwardedRef : null}
              autoFocus={i === 0 ? this.props.autofocus : null}
              onKeyPress={this.handleKeyPress}
            />
          );
        })}
      </div>
    );
  };
}

const StyledMultipleValuesRow = withStyles(styles)(MultipleValuesRow);
export default StyledMultipleValuesRow;
