import React, { Component } from 'react';
import PropTypes from 'prop-types';
import T from 'i18n-react';
import classnames from 'classnames';
import { UncontrolledTooltip } from 'reactstrap';
import { preventPropagation, connectWithStore } from 'services/helpers';
import { setPopoverOffset } from 'components/DataPrep/helper';
import DataPrepStore from 'components/DataPrep/store';
import ScrollableList from 'components/ScrollableList';
import {
  setLoading,
  setError,
  initializeMapToTarget,
  setTargetDataModel,
  setTargetModel
} from 'components/DataPrep/store/DataPrepActionCreator';

require('./MapToTarget.scss');

const PREFIX = 'features.DataPrep.Directives.MapToTarget';

class MapToTarget extends Component {
  static propTypes = {
    isOpen: PropTypes.bool,
    isDisabled: PropTypes.bool,
    column: PropTypes.string,
    onComplete: PropTypes.func,
    dataModelList: PropTypes.array,
    targetDataModel: PropTypes.object,
    targetModel: PropTypes.object,
  };

  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(
      this,
      document.getElementById('map-to-target-directive')
    );

    (async () => {
      setLoading(true);
      try {
        await initializeMapToTarget();
      } catch (error) {
        setError(error);
      } finally {
        setLoading(false);
      }
    })();
  }

  componentDidUpdate(prevProps, prevState, snapshot) {
    if (this.props.isOpen && !this.props.isDisabled && this.calculateOffset) {
      this.calculateOffset();
    }
  }

  async applyDirective(field) {
    // TODO
  }

  async selectTargetDataModel(dataModel) {
    setLoading(true);
    try {
      await setTargetDataModel(dataModel);
    } catch (error) {
      setError(error);
    } finally {
      setLoading(false);
    }
  }

  async selectTargetModel(model) {
    setLoading(true);
    try {
      await setTargetModel(model);
    } catch (error) {
      setError(error);
    } finally {
      setLoading(false);
    }
  }

  async selectTargetOption(option) {
    const { targetDataModel, targetModel } = this.props;
    if (targetDataModel) {
      if (targetModel) {
        await this.applyDirective(option);
      } else {
        await this.selectTargetDataModel(option);
      }
    } else {
      await this.selectTargetModel(option);
    }
  }

  renderDetail() {
    if (!this.props.isOpen || this.props.isDisabled) {
      return null;
    }

    let options;
    const selection = [];
    const { dataModelList, targetDataModel, targetModel } = this.props;

    if (targetDataModel) {
      selection.push(
        {
          key: 'datamodel',
          unselectFn: () => this.selectTargetDataModel(null),
          ...targetDataModel,
        }
      );
      if (targetModel) {
        selection.push(
          {
            key: 'model',
            unselectFn: () => this.selectTargetModel(null),
            ...targetModel,
          }
        );
        options = targetModel.fields || [];
      } else {
        options = targetDataModel.models || [];
      }
    } else {
      options = dataModelList || [];
    }

    return (
      <div className='second-level-popover' onClick={preventPropagation}>
        {selection.length === 0 ? <h5>{T.translate(`${PREFIX}.dataModelPlaceholder`)}</h5> : null}
        {selection.map(item => (
          <div id={`selected-item-${item.key}`} key={item.key} className='selected-item'>
            <span className='selected-item-name'>{item.name}</span>
            <span className='unselect-icon fa fa-times' onClick={item.unselectFn} />
            <UncontrolledTooltip target={`selected-item-${item.key}`}>
              {item.description || item.name}
            </UncontrolledTooltip>
          </div>
        ))}
        <hr />
        <ScrollableList>
          {options.map(option => (
            <div
              id={`target-option-${option.id}`}
              key={option.id}
              className='target-option'
              onClick={() => this.selectTargetOption(option)}
            >
              {option.name}
              <UncontrolledTooltip target={`target-option-${option.id}`}>
                {option.description || option.name}
              </UncontrolledTooltip>
            </div>
          ))}
        </ScrollableList>
      </div>
    );
  }

  render() {
    return (
      <div
        id='map-to-target-directive'
        className={classnames('map-to-target-directive clearfix action-item', {
          active: this.props.isOpen && !this.props.isDisabled,
          disabled: this.props.isDisabled,
        })}
      >
        <span>{T.translate(`${PREFIX}.title`)}</span>

        <span className='float-right'>
          <span className='fa fa-caret-right' />
        </span>

        {this.renderDetail()}
      </div>
    );
  }

}

const mapStateToProps = state => {
  const { dataModelList, targetDataModel, targetModel } = state.dataprep;
  return {
    dataModelList,
    targetDataModel,
    targetModel,
  };
};

export default connectWithStore(DataPrepStore, MapToTarget, mapStateToProps);
