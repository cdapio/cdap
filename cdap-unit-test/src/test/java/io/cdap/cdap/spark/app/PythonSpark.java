/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.spark.app;

import io.cdap.cdap.api.spark.AbstractSpark;
import io.cdap.cdap.api.spark.Spark;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * A {@link Spark} program that runs Python.
 */
public class PythonSpark extends AbstractSpark {

  private static final String SITECUSTOMIZE_PY =
      "import sys, types, typing, dis\n"
      + "sys.modules['typing.io'] = typing\n"
      + "sys.modules['typing.re'] = typing\n"
      + "try:\n"
      + "    import pyspark.cloudpickle.cloudpickle as cp\n"
      + "    import pyspark.cloudpickle.cloudpickle_fast as cp_fast\n"
      + "    if hasattr(types.CodeType, 'co_exceptiontable') or sys.version_info >= (3, 11):\n"
      + "        _orig_code_reduce = cp_fast._code_reduce\n"
      + "        def _code_reduce(obj):\n"
      + "            if hasattr(obj, 'co_exceptiontable'):\n"
      + "                return types.CodeType, (\n"
      + "                    obj.co_argcount, obj.co_posonlyargcount,\n"
      + "                    obj.co_kwonlyargcount, obj.co_nlocals, obj.co_stacksize,\n"
      + "                    obj.co_flags, obj.co_code, obj.co_consts, obj.co_names,\n"
      + "                    obj.co_varnames, obj.co_filename, obj.co_name, obj.co_qualname,\n"
      + "                    obj.co_firstlineno, obj.co_linetable, obj.co_exceptiontable,\n"
      + "                    obj.co_freevars, obj.co_cellvars\n"
      + "                )\n"
      + "            return _orig_code_reduce(obj)\n"
      + "        cp_fast._code_reduce = _code_reduce\n"
      + "        cp_fast.CloudPickler._dispatch_table[types.CodeType] = _code_reduce\n"
      + "        def _walk_global_ops(code):\n"
      + "            for instr in dis.get_instructions(code):\n"
      + "                op = instr.opcode\n"
      + "                if op in cp.GLOBAL_OPS:\n"
      + "                    is_lg = (op == cp.LOAD_GLOBAL and sys.version_info >= (3, 11))\n"
      + "                    arg = instr.arg >> 1 if is_lg else instr.arg\n"
      + "                    yield op, arg\n"
      + "        cp._walk_global_ops = _walk_global_ops\n"
      + "        cp_fast._walk_global_ops = _walk_global_ops\n"
      + "except Exception:\n"
      + "    pass\n";

  @Override
  protected void initialize() throws Exception {
    File siteCustomizeZip = File.createTempFile("sitecustomize", ".zip");
    siteCustomizeZip.deleteOnExit();
    try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(siteCustomizeZip))) {
      zos.putNextEntry(new ZipEntry("sitecustomize.py"));
      zos.write(SITECUSTOMIZE_PY.getBytes(StandardCharsets.UTF_8));
      zos.closeEntry();
    }
    getContext().setPySparkScript(getClass().getClassLoader().getResource("testPySpark.py").toURI(),
                                  siteCustomizeZip.toURI());
  }
}
