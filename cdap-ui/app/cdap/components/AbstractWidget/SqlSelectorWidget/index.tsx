/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import React from 'react';
import isEqual from 'lodash/isEqual';
import classnames from 'classnames';
import { objectQuery } from 'services/helpers';
import T from 'i18n-react';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import Button from '@material-ui/core/Button';
import ThemeWrapper from 'components/ThemeWrapper';
import SchemaContainer from 'components/AbstractWidget/SqlSelectorWidget/SchemaContainer';
import If from 'components/If';
import { IWidgetProps } from 'components/AbstractWidget';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';

const I18N_PREFIX = 'features.AbstractWidget.SqlSelectorWidget';

export const styles = () => {
  return {
    root: {
      padding: '4px',
      display: 'flex',
      'flex-direction': 'column',
    },
    buttonContainer: {
      display: 'flex',
      justifyContent: 'flex-end',
      '> button': {
        marginLeft: '5px',
      },
    },
    errorMessage: {
      margin: '5px',
    },
    emptyMessage: {
      margin: '0px',
    },
  };
};

interface ISqlSelectorProps extends IWidgetProps<null>, WithStyles<typeof styles> {}

export interface IFieldSchema {
  selected: boolean;
  name: string;
  alias: string;
}
export interface IParsedInputSchema {
  key: string;
  name: string;
  expanded: boolean;
  schema: IFieldSchema[];
}
interface ISqlSelectorWidgetState {
  expandAll: boolean;
  errors: { stageCountMap: object; message: string; exist: boolean };
  parsedInputSchemas: IParsedInputSchema[];
  aliases: object;
  modelCopy: string;
}

class SqlSelectorWidgetView extends React.PureComponent<
  ISqlSelectorProps,
  ISqlSelectorWidgetState
> {
  public state = {
    expandAll: false,
    errors: {
      stageCountMap: {},
      message: 'Please create one or more aliases for duplicate field names.',
      exist: false,
    },
    parsedInputSchemas: [],
    aliases: {},
    modelCopy: null,
  };

  public componentDidMount() {
    this.setState({ modelCopy: this.props.value ? this.props.value.toString() : '' }, () =>
      this.init(this.state.modelCopy)
    );
  }

  private toggleExpandAll = () => {
    this.state.expandAll ? this.collapseAllStages() : this.expandAllStages();
    this.setState({ expandAll: !this.state.expandAll });
  };

  private resetAll = () => {
    this.setState(
      {
        expandAll: false,
        parsedInputSchemas: [],
      },
      () => {
        this.init(this.state.modelCopy);
      }
    );
  };

  private expandStage = (curStage) => {
    this.setState(
      {
        parsedInputSchemas: this.state.parsedInputSchemas.map((stage: IParsedInputSchema) => {
          if (isEqual(stage, curStage)) {
            stage.expanded = !curStage.expanded;
          }
          return stage;
        }),
      },
      () => {
        // If all of the expansion panels are individually opened or closed,
        // this will change button expand all button text.
        let expandedCount = 0;
        this.state.parsedInputSchemas.forEach((stage: IParsedInputSchema) => {
          if (stage.expanded) {
            expandedCount++;
          }
        });
        if (expandedCount === this.state.parsedInputSchemas.length) {
          this.setState({ expandAll: true });
        } else if (expandedCount === 0) {
          this.setState({ expandAll: false });
        }
      }
    );
  };

  private expandAllStages = () => {
    this.setState({
      parsedInputSchemas: this.state.parsedInputSchemas.map((stage: IParsedInputSchema) => {
        stage.expanded = true;
        return stage;
      }),
    });
  };

  private collapseAllStages = () => {
    this.setState({
      parsedInputSchemas: this.state.parsedInputSchemas.map((stage: IParsedInputSchema) => {
        stage.expanded = false;
        return stage;
      }),
    });
  };

  private getStageError = () => {
    const stageCountMap = {};
    this.state.parsedInputSchemas.forEach((stage: IParsedInputSchema) => {
      stage.schema.forEach((field: IFieldSchema) => {
        if (this.state.aliases[field.alias] > 1) {
          if (!stageCountMap[stage.name]) {
            stageCountMap[stage.name] = 1;
          } else {
            stageCountMap[stage.name]++;
          }
        }
      });
    });
    this.setState({
      errors: {
        ...this.state.errors,
        stageCountMap,
      },
    });
  };

  private formatOutput = () => {
    const outputArr = [];
    const tempAliases = {};
    let errorExists = false;
    this.state.parsedInputSchemas.forEach((stage) => {
      stage.schema.forEach((field: IFieldSchema) => {
        if (!field.selected) {
          return;
        }
        const outputField = `${stage.name}.${field.name}${field.alias ? ` as ${field.alias}` : ''}`;
        if (!tempAliases[field.alias]) {
          tempAliases[field.alias] = 1;
        } else {
          tempAliases[field.alias]++;
          errorExists = true;
        }
        outputArr.push(outputField);
      });
    });
    this.setState(
      {
        errors: {
          stageCountMap: this.state.errors.stageCountMap,
          message: this.state.errors.message,
          exist: errorExists,
        },
        aliases: tempAliases,
      },
      this.getStageError
    );

    this.props.onChange(outputArr.join(','));
  };

  private init = (inputModel) => {
    const initialModel = {};
    const parsedInputSchemas: IParsedInputSchema[] = [];
    if (inputModel) {
      inputModel.split(',').forEach((entry) => {
        const split = entry.split(' as ');
        const fieldInfo = split[0].split('.');
        if (fieldInfo.length > 1) {
          const stageName = fieldInfo[0];
          const fieldName = fieldInfo[1];
          if (!initialModel[stageName]) {
            initialModel[stageName] = {};
          }
          initialModel[stageName][fieldName] = split[1] ? split[1] : true;
        }
      });
    }

    const inputSchema = objectQuery(this.props, 'extraConfig', 'inputSchema') || [];

    inputSchema.forEach((input, i) => {
      let schema;
      try {
        schema = JSON.parse(input.schema);
      } catch (e) {
        schema = {
          fields: [],
        };
      }
      schema = schema.fields.map((field) => {
        if (objectQuery(initialModel, input.name, field.name)) {
          field.selected = true;
          field.alias =
            initialModel[input.name][field.name] === true
              ? ''
              : initialModel[input.name][field.name];
        } else {
          field.selected = inputModel ? false : true;
          field.alias = field.name;
        }

        return field;
      });
      parsedInputSchemas.push({
        key: `${i}-${input.name}`,
        name: input.name,
        schema,
        expanded: false,
      });
    });
    this.setState({ parsedInputSchemas }, () => {
      // there is some timing issue with setting this default value when the modal first open
      setTimeout(this.formatOutput, 100);
    });
  };

  private onSchemaChange = (newStage) => {
    const newIpSchema = this.state.parsedInputSchemas.map((stage) => {
      if (stage.key === newStage.key) {
        return newStage;
      }
      return stage;
    });
    this.setState({ parsedInputSchemas: newIpSchema }, this.formatOutput);
  };

  public render() {
    const { classes, errors } = this.props;
    return (
      <div className={classes.root}>
        <If condition={this.state.parsedInputSchemas.length > 0}>
          <div className={classes.buttonContainer}>
            <Button variant="outlined" size="small" onClick={this.toggleExpandAll}>
              {this.state.expandAll
                ? T.translate(`${I18N_PREFIX}.collapseAll`)
                : T.translate(`${I18N_PREFIX}.expandAll`)}
            </Button>
            <Button disabled={this.props.disabled} size="small" onClick={this.resetAll}>
              {T.translate(`${I18N_PREFIX}.resetAll`)}
            </Button>
          </div>
        </If>
        <If condition={this.state.errors.exist}>
          <div className={classnames('text-danger', classes.errorMessage)}>
            {this.state.errors.message}
          </div>
        </If>
        <If condition={this.state.parsedInputSchemas.length > 0}>
          <React.Fragment>
            {this.state.parsedInputSchemas.map((stage: IParsedInputSchema) => {
              return (
                <SchemaContainer
                  key={stage.key}
                  onExpandClick={this.expandStage}
                  onSchemaChange={this.onSchemaChange}
                  stage={stage}
                  aliases={this.state.aliases}
                  errorCount={this.state.errors.stageCountMap[stage.name]}
                  disabled={this.props.disabled}
                  errors={errors}
                />
              );
            })}
          </React.Fragment>
        </If>
        <If condition={this.state.parsedInputSchemas.length === 0}>
          <h4 className={classes.emptyMessage}>No input stages</h4>
        </If>
      </div>
    );
  }
}

const StyledSqlSelectorWidget = withStyles(styles)(SqlSelectorWidgetView);

export default function SqlSelectorWidget(props) {
  return (
    <ThemeWrapper>
      <StyledSqlSelectorWidget {...props} />
    </ThemeWrapper>
  );
}

(SqlSelectorWidget as any).propTypes = WIDGET_PROPTYPES;
