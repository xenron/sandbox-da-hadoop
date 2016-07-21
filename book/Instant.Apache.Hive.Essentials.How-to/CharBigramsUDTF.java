package com.leeriggins.hive;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.STRING;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

import org.apache.hadoop.hive.ql.exec.Description;

@Description(
  name="char_bigram",
  value="_FUNC_(string) - outputs each pair of consecutive characters in each word of the input string")
public class CharBigramsUDTF extends GenericUDTF {

  private StringObjectInspector inputOI;

  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length != 1) {
      throw new UDFArgumentLengthException("Expected 1 argument, received " + args.length);
    }

    ObjectInspector oi = args[0];
    if (oi.getCategory() != PRIMITIVE || ((PrimitiveObjectInspector) oi).getPrimitiveCategory() != STRING) {
      throw new UDFArgumentTypeException(0, "Argument must be a string.");
    }

    inputOI = (StringObjectInspector) oi;

    List<String> fieldNames = Arrays.asList("bigram");
    List<ObjectInspector> fieldOIs = Arrays.<ObjectInspector> asList(javaStringObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    String content = inputOI.getPrimitiveJavaObject(args[0]);
    String[] words = content.split("\\s+");
    for (String word : words) {
      if (word.length() > 1) {
        for (int i = 0; i < word.length() - 2; i++) {
          forward(new Object[] { word.substring(i, i + 2) });
        }
      }
    }
  }

  @Override
  public void close() throws HiveException {
    // do nothing
  }

}
