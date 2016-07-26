package com.leeriggins.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.commons.codec.digest.DigestUtils;

@Description(
    name = "md5sum",
    value = "_FUNC_(string) - computes the MD5 sum of the input string.  Returns the hash in hexidecimal format.",
    extended = "Example: _FUNC_('foobar') = 3858f62230ac3c915f300c664312c63f")
public class MD5SumUDF extends UDF {

  public String evaluate(String input) {
    return DigestUtils.md5Hex(input);
  }

}
