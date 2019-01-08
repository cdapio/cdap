/*
 * Copyright © 2018 Cask Data, Inc.
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
package co.cask.cdap.common.enums;

/**
 * @author bhupesh.goel
 *
 */
public enum FeatureSTATS {

    Feature("Feature", "string", ""), 
    Variance("Variance", "double", "numeric"), 
    Max("Max", "double", "numeric"), 
    Min("Min", "double", "numeric"), 
    Mean("Mean", "double", "numeric"), 
    NumOfNonZeros("Num of Non Zeros", "double", "numeric"),
    NumOfNulls("Num of Nulls", "long", "numeric"),
    NormL1("Norm L1", "double", "numeric"),
    NormL2("Norm L2", "double", "numeric"),
    TwentyFivePercentile("TwentyFive Percentile", "double", "numeric"),
    FiftyPercentile("Fifty Percentile", "double", "numeric"),
    SeventyFivePercentile("SeventyFive Percentile", "double", "numeric"),
    InterQuartilePercentile("Inter Quartile Percentile", "double", "numeric"),
    LeastFrequentWordCount("Least Frequent Word Count", "long", "string"),
    TotalCount("Total Count", "long", "string"),
    UniqueCount("Unique Count", "long", "string"),
    MostFrequentWordCount("Most Frequent Word Count", "long", "string"),
    MostFrequentEntry("Most Frequent Entry", "string", "string");

    private final String name;
    private final String type;
    private final String inputType;

    FeatureSTATS(final String name, final String type, final String inputType) {
        this.name = name;
        this.type = type;
        this.inputType = inputType;
    }

    public boolean liesInBetween(Object value, Object minValue, Object maxValue) {
        if (value == null || minValue == null || maxValue == null) {
            return false;
        }
        switch (type) {
        case "double":
            Double doubleValue = getDoubleValue(value);
            Double doubleMinValue = getDoubleValue(minValue);
            Double doubleMaxValue = getDoubleValue(maxValue);
            if (doubleValue.compareTo(doubleMinValue) < 0 || doubleValue.compareTo(doubleMaxValue) > 0) {
                return false;
            }
            break;
        case "long":
            Long longValue = getLongValue(value);
            Long longMinValue = getLongValue(minValue);
            Long longMaxValue = getLongValue(maxValue);
            if (longValue.compareTo(longMinValue) < 0 || longValue.compareTo(longMaxValue) > 0) {
                return false;
            }
            break;
        case "string":
            if (value.toString().compareTo(minValue.toString()) < 0
                    || value.toString().compareTo(maxValue.toString()) > 0) {
                return false;
            }
            break;
        case "boolean":
            Boolean boolValue = getBooleanValue(value);
            Boolean boolMinValue = getBooleanValue(value);
            Boolean boolMaxValue = getBooleanValue(maxValue);
            if (boolValue.compareTo(boolMinValue) < 0 || boolValue.compareTo(boolMaxValue) > 0) {
                return false;
            }
            break;
        }
        return true;
    }

    private Long getLongValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Double) {
            return ((Double) value).longValue();
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof String) {
            return Long.parseLong(value.toString());
        }
        return (Long) value;
    }

    private Boolean getBooleanValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value.toString().equalsIgnoreCase("true")) {
            return true;
        } else if (value.toString().equalsIgnoreCase("false")) {
            return false;
        }
        return null;
    }

    private Double getDoubleValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Double) {
            return (Double) value;
        }
        if (value instanceof Long) {
            return ((Long) value).longValue() * 1.0;
        }
        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        return (Double) value;
    }

    public int compare(Object value1, Object value2) {
        if (value1 == null) {
            return -1;
        } else if (value2 == null) {
            return 1;
        }
        switch (type) {
        case "double":
            Double doubleValue1 = getDoubleValue(value1);
            Double doubleValue2 = getDoubleValue(value2);
            return doubleValue1.compareTo(doubleValue2);
        case "long":
            Long longValue1 = getLongValue(value1);
            Long longValue2 = getLongValue(value2);
            return longValue1.compareTo(longValue2);
        case "boolean":
            Boolean boolValue1 = getBooleanValue(value1);
            Boolean boolValue2 = getBooleanValue(value2);
            return boolValue1.compareTo(boolValue2);
        default:
            return value1.toString().compareTo(value2.toString());
        }

    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * @return the inputType
     */
    public String getInputType() {
        return inputType;
    }
}
