import React from 'react';
import PropTypes from 'prop-types';
import classnames from 'classnames';

import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';

import ThemeWrapper from 'components/ThemeWrapper';
import If from 'components/If';
import Rule from 'components/AbstractWidget/SqlConditionsWidget/Rule';

export const styles = () => {
  return {
    root: {
      width: '100%',
    },
    emptyMessage: {
      marginBottom: 0,
    },
    textDanger: { 'white-space': 'pre-line' },
    rulesContainer: {
      display: 'flex',
      width: '100%',
      'flex-direction': 'column',
    },
  };
};

export interface IStage {
  fieldName: string;
  stageName: string;
}

export interface IRule extends Array<IStage> {}

interface IRules extends Array<IRule> {}

export interface IInputSchema {
  [key: string]: string[];
}

interface ISqlConditionsWidgetProps extends WithStyles<typeof styles> {
  value: string;
  inputSchema: any[];
  onChange: (arg0: any) => void;
  disabled: boolean;
}

interface ISqlConditionsWidgetState {
  warning: string;
  error: string;
  stageList: string[];
  rules: IRules;
  mapInputSchema: IInputSchema;
}

class SqlConditionsWidgetView extends React.Component<
  ISqlConditionsWidgetProps,
  ISqlConditionsWidgetState
> {
  public state = {
    warning: null,
    error: null,
    stageList: [],
    rules: [],
    mapInputSchema: {},
  };

  public checkRulesForValidStageNames = () => {
    const invalidRule = /[&\.=]/g;
    let invalidStageNames = [];
    this.state.rules.forEach((rule) => {
      const invalidStageName = rule.filter((field) => invalidRule.test(field.stageName));
      if (invalidStageName.length) {
        invalidStageNames = invalidStageNames.concat(
          invalidStageName.map((stage) => stage.stageName)
        );
      }
    });
    if (invalidStageNames.length) {
      const error = `Invalid name for input ${
        invalidStageNames.length > 1 ? 'nodes' : 'node'
      }: ${invalidStageNames
        .map((sn) => JSON.stringify(sn))
        .join(', ')}. \n Node names cannot contain "&" "=" "."`;
      this.setState({
        error,
      });
    }
    return this.state.error && this.state.error.length;
  };

  public formatOutput = () => {
    this.checkRulesForValidStageNames();
    if (this.state.stageList.length < 2) {
      this.props.onChange('');
      return;
    }
    const outputArr = [];
    this.state.rules.forEach((rule) => {
      const ruleCheck = rule.filter((field) => {
        return !field.fieldName;
      });
      if (ruleCheck.length > 0) {
        return;
      }
      const ruleArr = rule.map((field) => {
        return field.stageName + '.' + field.fieldName;
      });
      outputArr.push(ruleArr.join(' = '));
    });
    this.props.onChange(outputArr.join(' & '));
  };

  public addRule = () => {
    if (this.state.stageList.length === 0) {
      return;
    }
    const rules = [];
    this.state.stageList.forEach((stage) => {
      rules.push({
        stageName: stage,
        fieldName: this.state.mapInputSchema[stage][0],
      });
    });
    this.setState({ rules: [...this.state.rules, rules] }, () => {
      this.formatOutput();
    });
  };

  public deleteRule = (index: number) => {
    const rulesCopy = [...this.state.rules];
    rulesCopy.splice(index, 1);
    this.setState({ rules: rulesCopy }, () => {
      this.formatOutput();
    });
  };

  public updateRule = (changedRule: IRule, ruleIdx: number) => {
    const rules = this.state.rules.map((rule, i) => {
      if (i === ruleIdx) {
        return changedRule;
      }
      return rule;
    });
    this.setState({ rules }, () => {
      this.formatOutput();
    });
  };

  public initializeOptions = () => {
    return new Promise((resolve, reject) => {
      const stageList = [];
      const mapInputSchema = {};
      this.props.inputSchema.forEach((input) => {
        stageList.push(input.name);
        try {
          mapInputSchema[input.name] = JSON.parse(input.schema).fields.map((field) => {
            return field.name;
          });
        } catch (e) {
          mapInputSchema[input.name] = [];
          this.setState(
            {
              error: 'Error parsing input schemas.',
              stageList,
              mapInputSchema,
            },
            () => {
              reject(e);
            }
          );
        }
      });
      if (stageList.length < 2) {
        this.setState({ error: 'Please connect 2 or more stages.' });
      }
      this.setState({ stageList, mapInputSchema }, () => {
        resolve();
      });
    });
  };

  public init = () => {
    this.initializeOptions()
      .then(() => {
        if (!this.props.value) {
          this.addRule();
          return;
        }
        const modelSplit = this.props.value.split('&').map((rule) => {
          return rule.trim();
        });
        modelSplit.forEach((rule) => {
          const rulesArr = [];
          rule.split('=').forEach((field) => {
            const splitField = field.trim().split('.');
            // Not including rule if stage has been disconnected
            if (this.state.stageList.indexOf(splitField[0]) === -1) {
              return;
            }
            rulesArr.push({
              stageName: splitField[0],
              fieldName: splitField[1],
            });
          });
          // Missed fields scenario will happen if the user connects more stages into the join node
          // after they have configured join conditions previously
          const missedFields = this.state.stageList.filter((stage) => {
            const filteredRule = rulesArr.filter((field) => {
              return field.stageName === stage;
            });
            return filteredRule.length === 0 ? true : false;
          });
          if (missedFields.length > 0) {
            missedFields.forEach((field) => {
              rulesArr.push({
                stageName: field,
                fieldName: this.state.mapInputSchema[field][0],
              });
            });
            this.setState({
              warning:
                "Input stages have changed since the last time you edit this node's configuration. Please verify the condition is still valid.",
            });
          }
          this.setState({ rules: [...this.state.rules, rulesArr] });
        });
      })
      .then(() => {
        this.formatOutput();
      })
      .catch((e) => {
        // Error is not logged on console as that causes linter issue,
        // previously we logged on console in angular.
      });
  };

  public componentDidMount() {
    this.init();
  }

  public render() {
    const { classes } = this.props;
    return (
      <div className={classes.root}>
        <If condition={this.state.error && this.state.stageList.length > 0}>
          <div className={classnames(classes.textDanger, 'text-danger')}>{this.state.error}</div>
        </If>
        <If condition={this.state.warning && !this.state.error}>
          <div className="text-warning">{this.state.warning}</div>
        </If>
        <div className={classes.rulesContainer}>
          {this.state.rules.map((rule, i) => {
            return (
              <Rule
                rule={rule}
                ruleIdx={i}
                rulesCount={this.state.rules.length}
                inputSchema={this.state.mapInputSchema}
                addRule={this.addRule}
                disabled={this.props.disabled}
                updateRule={(changedRule) => this.updateRule(changedRule, i)}
                deleteRule={this.deleteRule}
              />
            );
          })}
        </div>
        <If condition={this.state.stageList.length === 0}>
          <h4 className={classes.emptyMessage}>No input stages</h4>
        </If>
      </div>
    );
  }
}

const SqlConditionsWidget = withStyles(styles)(SqlConditionsWidgetView);

export default function StyledSqlConditionsWidget(props) {
  return (
    <ThemeWrapper>
      <SqlConditionsWidget {...props} />
    </ThemeWrapper>
  );
}

(StyledSqlConditionsWidget as any).propTypes = {
  value: PropTypes.string,
  inputSchema: PropTypes.object,
  onChange: PropTypes.func,
  disabled: PropTypes.bool,
};
