package com.weibo.dip.data.platform.datacubic;

import java.io.InputStream;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang.StringUtils;
import org.python.core.PyFunction;
import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

/** @author yurun */
public class JPythonTester {
  public static String getPythons() throws Exception {
    InputStream in = JPythonTester.class.getClassLoader().getResourceAsStream("pythons");

    List<String> lines = IOUtils.readLines(in, CharEncoding.UTF_8);

    return StringUtils.join(lines, "\n");
  }

  public static void main(String[] args) throws Exception {
    PythonInterpreter interpreter = new PythonInterpreter();

    interpreter.exec(getPythons());

    PyFunction func = interpreter.get("adder", PyFunction.class);

    PyObject result = func.__call__(new PyInteger(1), new PyInteger(2));

    System.out.println(result.asInt());
  }
}
