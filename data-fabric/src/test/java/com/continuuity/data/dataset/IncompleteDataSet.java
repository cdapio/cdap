package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.dataset.table.Table;

// this dataset is missing the constructor from data set spec
public class IncompleteDataSet extends DataSet {
  public IncompleteDataSet(String name) {
    super(name);
  }
  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this).
        dataset(new Table("t_" + getName()).configure()).
        create();
  }
}