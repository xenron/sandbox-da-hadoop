package com.leeriggins.hive;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.BINARY;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.STRING;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

@Description(
    name = "md5sum",
    value = "_FUNC_(string|byte, string|byte, ...) - computes the MD5 sum of the inputs.  Returns the hash in hexidecimal format.",
    extended = "_FUNC_(string|byte, string|byte, ...) - computes the MD5 sum of the inputs.  Returns the hash in hexidecimal format.\n\nExample: _FUNC_('foo', 'bar') = 3858f62230ac3c915f300c664312c63f")
public class GenericMD5SumUDF extends GenericUDF {

  private PrimitiveObjectInspector[] inputOIs;

  @Override
  public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args == null || args.length == 0) {
      throw new UDFArgumentLengthException("Must supply at least one argument.");
    }
    inputOIs = new PrimitiveObjectInspector[args.length];

    for (int i = 0; i < args.length; i++) {
      ObjectInspector objectInspector = args[i];
      if (objectInspector.getCategory().equals(PRIMITIVE)) {
        PrimitiveObjectInspector oi = (PrimitiveObjectInspector) objectInspector;
        PrimitiveCategory cat = oi.getPrimitiveCategory();
        if (cat.equals(STRING) || cat.equals(BINARY)) {
          inputOIs[i] = oi;
        } else {
          throw new UDFArgumentTypeException(i, String.format(
            "Expected a string or byte argument, received a %s at index %d",
            oi.getPrimitiveCategory(),
            i));
        }
      } else {
        throw new UDFArgumentTypeException(i, String.format(
          "Expected a string or byte argument, received a %s at index %d",
          objectInspector.getCategory(),
          i));
      }
    }

    return javaStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] args) throws HiveException {
    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      for (int i = 0; i < args.length; i++) {
        DeferredObject arg = args[i];
        PrimitiveObjectInspector inspector = inputOIs[i];
        if (inspector.getPrimitiveCategory().equals(STRING)) {
          String content = ((StringObjectInspector) inspector).getPrimitiveJavaObject(arg.get());
          digest.update(content.getBytes());
        } else {
          ByteArrayRef ref = (ByteArrayRef) inspector.getPrimitiveJavaObject(arg);
          digest.update(ref.getData());
        }
      }
      return Hex.encodeHexString(digest.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new HiveException(e);
    }
  }

  @Override
  public String getDisplayString(String[] args) {
    StringBuilder builder = new StringBuilder();
    builder.append("md5sum(");
    for (String arg : args) {
      builder.append(arg).append(",");
    }
    builder.delete(builder.length() - 1, builder.length());
    builder.append(")");
    return builder.toString();
  }

}
