package com.leeriggins.hive;

import org.apache.hadoop.hive.ql.exec.*;

@Description(name = "regression_slope", value = "_FUNC_(double x, double y) - computes the slope of the simple linear regression")
public class SimpleRegressionSlopeUDAF extends UDAF {

  private SimpleRegressionSlopeUDAF() {
    // cannot be instantiated
  }

  public static class SimpleRegressionState {
    double sumX = 0.0;
    double sumY = 0.0;
    double sumX2 = 0.0;
    double sumXY = 0.0;
    int n = 0;
  }

  public static class SimpleRegressionEvaluator implements UDAFEvaluator {

    private SimpleRegressionState state;

    public SimpleRegressionEvaluator() {
      super();
      state = new SimpleRegressionState();
      init();
    }

    @Override
    public void init() {
      state.sumX = 0.0;
      state.sumY = 0.0;
      state.sumX2 = 0.0;
      state.sumXY = 0.0;
      state.n = 0;
    }

    public boolean iterate(Double x, Double y) {
      state.sumX += x;
      state.sumY += y;
      state.sumX2 += x * x;
      state.sumXY += x * y;
      state.n += 1;
      return true;
    }

    public SimpleRegressionState terminatePartial() {
      return state;
    }

    public boolean merge(SimpleRegressionState toMerge) {
      if (toMerge != null) {
        state.sumX += toMerge.sumX;
        state.sumY += toMerge.sumY;
        state.sumX2 += toMerge.sumX2;
        state.sumXY += toMerge.sumXY;
        state.n += toMerge.n;
      }
      return true;
    }

    public Double terminate() {
      double numerator = state.sumXY - state.sumX * state.sumY / state.n;
      double denominator = state.sumX2 - state.sumX * state.sumX / state.n;

      double slope = numerator / denominator;

      return slope;
    }

  }
}