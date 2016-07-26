package com.leeriggins.hive;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.*;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.DOUBLE;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

import java.util.*;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

@Description(
  name="regression",
  value="_FUNC_(double x,double y) - computes the simple linear regression")
public class SimpleRegressionUDAF extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
    ObjectInspector[] inputOIs = info.getParameterObjectInspectors();
    if (inputOIs.length != 2) {
      throw new UDFArgumentLengthException("Expected 2 arguments, received " + inputOIs.length);
    }
    for (int i = 0; i < inputOIs.length; i++) {
      ObjectInspector oi = inputOIs[i];
      if (oi.getCategory() != PRIMITIVE || ((PrimitiveObjectInspector) oi).getPrimitiveCategory() != DOUBLE) {
        throw new UDFArgumentTypeException(i, "This function requires double arguments.");
      }
    }
    return new SimpleRegressionUDAFEvaluator();
  }

  private static class SimpleRegressionAggregationBuffer implements AggregationBuffer {
    double sumX = 0.0;
    double sumX2 = 0.0;
    double sumY = 0.0;
    double sumXY = 0.0;
    int n = 0;
  }

  public static class SimpleRegressionUDAFEvaluator extends GenericUDAFEvaluator {

    private DoubleObjectInspector[] originalDataOIs = new DoubleObjectInspector[2];
    private ListObjectInspector partialDataOI;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      if (m.equals(Mode.PARTIAL1)) {
        processOriginalDataObjectInspectors(parameters);
        return ObjectInspectorFactory.getStandardListObjectInspector(javaDoubleObjectInspector);
      } else if (m.equals(Mode.COMPLETE)) {
        processOriginalDataObjectInspectors(parameters);
        return createOutputOI();
      } else if (m.equals(Mode.PARTIAL2)) {
        processPartialDataObjectInspectors(parameters);
        return ObjectInspectorFactory.getStandardListObjectInspector(javaDoubleObjectInspector);
      } else if (m.equals(Mode.FINAL)) {
        processPartialDataObjectInspectors(parameters);
        return createOutputOI();
      }
      throw new IllegalStateException("Unknown mode!");
    }

    private void processOriginalDataObjectInspectors(ObjectInspector[] parameters) throws HiveException {
      if (parameters.length != 2) {
        throw new UDFArgumentLengthException("Expected 2 arguments, received " + parameters.length);
      }
      for (int i = 0; i < parameters.length; i++) {
        ObjectInspector oi = parameters[i];
        if (oi.getCategory() != PRIMITIVE || ((PrimitiveObjectInspector) oi).getPrimitiveCategory() != DOUBLE) {
          throw new UDFArgumentTypeException(i, "This UDAF requires double arguments.");
        }
        originalDataOIs[i] = (DoubleObjectInspector) oi;
      }
    }

    private void processPartialDataObjectInspectors(ObjectInspector[] parameters) throws HiveException {
      if (parameters.length != 1) {
        throw new UDFArgumentLengthException("Expected 1 argument from partial data, received " + parameters.length);
      }
      if (parameters[0].getCategory() == LIST) {
        ObjectInspector elemOI = ((ListObjectInspector) parameters[0]).getListElementObjectInspector();
        if (elemOI.getCategory() == PRIMITIVE && ((PrimitiveObjectInspector) elemOI).getPrimitiveCategory() == DOUBLE) {
          partialDataOI = (ListObjectInspector) parameters[0];
        }
      }
    }

    private StructObjectInspector createOutputOI() {
      List<String> fieldNames = Arrays.asList("slope", "intercept");
      List<ObjectInspector> fieldOIs = Collections.nCopies(2, (ObjectInspector) javaDoubleObjectInspector);
      return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new SimpleRegressionAggregationBuffer();
    }

    @Override
    public void iterate(AggregationBuffer buffer, Object[] inputs) throws HiveException {
      SimpleRegressionAggregationBuffer buff = (SimpleRegressionAggregationBuffer) buffer;
      double x = (Double) originalDataOIs[0].getPrimitiveJavaObject(inputs[0]);
      double y = (Double) originalDataOIs[1].getPrimitiveJavaObject(inputs[1]);
      buff.sumX += x;
      buff.sumY += y;
      buff.sumX2 += x * x;
      buff.sumXY += x * y;
      buff.n += 1;
    }

    @Override
    public Object terminatePartial(AggregationBuffer buffer) throws HiveException {
      SimpleRegressionAggregationBuffer buff = (SimpleRegressionAggregationBuffer) buffer;
      return new Double[] { buff.sumX, buff.sumY, buff.sumX2, buff.sumXY, (double) buff.n };
    }

    private double getDouble(Object obj, int index) {
      DoubleObjectInspector elemOI = (DoubleObjectInspector) partialDataOI.getListElementObjectInspector();
      Object field = partialDataOI.getListElement(obj, index);
      return (Double) elemOI.getPrimitiveJavaObject(field);
    }

    @Override
    public void merge(AggregationBuffer buffer, Object partialAggregation) throws HiveException {
      SimpleRegressionAggregationBuffer buff = (SimpleRegressionAggregationBuffer) buffer;
      buff.sumX += getDouble(partialAggregation, 0);
      buff.sumY += getDouble(partialAggregation, 1);
      buff.sumX2 += getDouble(partialAggregation, 2);
      buff.sumXY += getDouble(partialAggregation, 3);
      buff.n += getDouble(partialAggregation, 4);
    }

    @Override
    public void reset(AggregationBuffer buffer) throws HiveException {
      SimpleRegressionAggregationBuffer aggBuffer = (SimpleRegressionAggregationBuffer) buffer;
      aggBuffer.sumX = 0.0;
      aggBuffer.sumX2 = 0.0;
      aggBuffer.sumY = 0.0;
      aggBuffer.sumXY = 0.0;
      aggBuffer.n = 0;
    }

    @Override
    public Object terminate(AggregationBuffer buffer) throws HiveException {
      SimpleRegressionAggregationBuffer aggBuffer = (SimpleRegressionAggregationBuffer) buffer;

      double numerator = aggBuffer.sumXY - aggBuffer.sumX * aggBuffer.sumY / aggBuffer.n;
      double denominator = aggBuffer.sumX2 - aggBuffer.sumX * aggBuffer.sumX / aggBuffer.n;
      double slope = numerator / denominator;

      double intercept = aggBuffer.sumY / aggBuffer.n - slope * aggBuffer.sumX / aggBuffer.n;
      return new Double[] { slope, intercept };
    }
  }

}
