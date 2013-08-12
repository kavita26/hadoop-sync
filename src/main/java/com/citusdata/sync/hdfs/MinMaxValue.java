package com.citusdata.sync.hdfs;


public class MinMaxValue {

  private final String minValue;
  private final String maxValue;

  /*
   * MinMaxValue creates a new min/max value object.
   */
  public MinMaxValue(String minValue, String maxValue) {
    this.minValue = minValue;
    this.maxValue = maxValue;
  }

  /*
   * getMinValue returns the minimum value.
   */
  public String getMinValue() {
    return minValue;
  }

  /*
   * getMaxValue returns the maximum value.
   */
  public String getMaxValue() {
    return maxValue;
  }
}
