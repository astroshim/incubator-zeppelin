#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os, sys, getopt, traceback, json, re

from py4j.java_gateway import java_import, JavaGateway, GatewayClient
from py4j.protocol import Py4JJavaError
import warnings
import ast
import traceback
import warnings


# for back compatibility

class Logger(object):
  def __init__(self):
    pass

  def write(self, message):
    f.write('write?? ------------------' + message + '\n')
    intp.appendOutput(message)

  def reset(self):
    pass

  def flush(self):
    pass


class PyZeppelinContext(dict):
  def __init__(self, zc):
    self.z = zc
    self._displayhook = lambda *args: None

  # By implementing special methods it makes operating on it more Pythonic
  def __setitem__(self, key, item):
    self.z.put(key, item)

  def __getitem__(self, key):
    return self.z.get(key)

  def __delitem__(self, key):
    self.z.remove(key)

  def __contains__(self, item):
    return self.z.containsKey(item)

  def add(self, key, value):
    self.__setitem__(key, value)

  def put(self, key, value):
    self.__setitem__(key, value)

  def get(self, key):
    return self.__getitem__(key)

  def input(self, name, defaultValue=""):
    return self.z.input(name, defaultValue)

  def select(self, name, options, defaultValue=""):
    # auto_convert to ArrayList doesn't match the method signature on JVM side
    tuples = list(map(lambda items: self.__tupleToScalaTuple2(items), options))
    iterables = gateway.jvm.scala.collection.JavaConversions.collectionAsScalaIterable(tuples)
    return self.z.select(name, defaultValue, iterables)

  def checkbox(self, name, options, defaultChecked=None):
    if defaultChecked is None:
      defaultChecked = list(map(lambda items: items[0], options))
    optionTuples = list(map(lambda items: self.__tupleToScalaTuple2(items), options))
    optionIterables = gateway.jvm.scala.collection.JavaConversions.collectionAsScalaIterable(optionTuples)
    defaultCheckedIterables = gateway.jvm.scala.collection.JavaConversions.collectionAsScalaIterable(defaultChecked)

    checkedIterables = self.z.checkbox(name, defaultCheckedIterables, optionIterables)
    return gateway.jvm.scala.collection.JavaConversions.asJavaCollection(checkedIterables)

  def registerHook(self, event, cmd, replName=None):
    if replName is None:
      self.z.registerHook(event, cmd)
    else:
      self.z.registerHook(event, cmd, replName)

  def unregisterHook(self, event, replName=None):
    if replName is None:
      self.z.unregisterHook(event)
    else:
      self.z.unregisterHook(event, replName)

  def getHook(self, event, replName=None):
    if replName is None:
      return self.z.getHook(event)
    return self.z.getHook(event, replName)

  def _setup_matplotlib(self):
    # If we don't have matplotlib installed don't bother continuing
    try:
      import matplotlib
    except ImportError:
      return
    
    # Make sure custom backends are available in the PYTHONPATH
    rootdir = os.environ.get('ZEPPELIN_HOME', os.getcwd())
    mpl_path = os.path.join(rootdir, 'interpreter', 'lib', 'python')
    if mpl_path not in sys.path:
      sys.path.append(mpl_path)
    
    # Finally check if backend exists, and if so configure as appropriate
    try:
      matplotlib.use('module://backend_zinline')
      import backend_zinline
      
      # Everything looks good so make config assuming that we are using
      # an inline backend
      self._displayhook = backend_zinline.displayhook
      self.configure_mpl(width=600, height=400, dpi=72, fontsize=10,
                         interactive=True, format='png', context=self.z)
    except ImportError:
      # Fall back to Agg if no custom backend installed
      matplotlib.use('Agg')
      warnings.warn("Unable to load inline matplotlib backend, "
                    "falling back to Agg")

  def configure_mpl(self, **kwargs):
    import mpl_config
    mpl_config.configure(**kwargs)

  def __tupleToScalaTuple2(self, tuple):
    if (len(tuple) == 2):
      return gateway.jvm.scala.Tuple2(tuple[0], tuple[1])
    else:
      raise IndexError("options must be a list of tuple of 2")


output = Logger()
sys.stdout = output
sys.stderr = output


client = GatewayClient(port=int(sys.argv[1]))

gateway = JavaGateway(client, auto_convert = True)
#gateway = JavaGateway(client)

intp = gateway.entry_point
intp.onPythonScriptInitialized()

#z = PyZeppelinContext(intp.getZeppelinContext())
#z._setup_matplotlib()

while True :
  req = intp.getStatements()
  try:
    stmts = req.statements().split("\n")
    jobGroup = req.jobGroup()
    final_code = []

    # Get post-execute hooks
    try:
      global_hook = intp.getHook('post_exec_dev')
    except:
      global_hook = None


    try:
      user_hook = z.getHook('post_exec')
    except:
      user_hook = None
      
    nhooks = 0
    for hook in (global_hook, user_hook):
      if hook:
        nhooks += 1

    for s in stmts:
      if s == None:
        continue

      # skip comment
      s_stripped = s.strip()
      if len(s_stripped) == 0 or s_stripped.startswith("#"):
        continue

      final_code.append(s)

    if final_code:
      # use exec mode to compile the statements except the last statement,
      # so that the last statement's evaluation will be printed to stdout
      #sc.setJobGroup(jobGroup, "Zeppelin")
      code = compile('\n'.join(final_code), '<stdin>', 'exec', ast.PyCF_ONLY_AST, 1)
      to_run_hooks = code.body[-nhooks:]

      print ("to_run_hooks : " + to_run_hooks)
      to_run_exec, to_run_single = (code.body[:-(nhooks + 1)],
                                    [code.body[-(nhooks + 1)]])
      print ("to_run_single : " + to_run_single)

      try:
        for node in to_run_exec:
          mod = ast.Module([node])
          code = compile(mod, '<stdin>', 'exec')
          exec(code)

        '''
        for node in to_run_single:
          mod = ast.Interactive([node])
          code = compile(mod, '<stdin>', 'single')
          exec(code)
        '''

        for node in to_run_hooks:
          mod = ast.Module([node])
          code = compile(mod, '<stdin>', 'exec')
          exec(code)
      except:
        raise Exception(traceback.format_exc())

    intp.setStatementsFinished("", False)
  except Py4JJavaError:
    excInnerError = traceback.format_exc() # format_tb() does not return the inner exception
    innerErrorStart = excInnerError.find("Py4JJavaError:")
    if innerErrorStart > -1:
       excInnerError = excInnerError[innerErrorStart:]
    intp.setStatementsFinished(excInnerError + str(sys.exc_info()), True)
  except:
    intp.setStatementsFinished(traceback.format_exc(), True)

  output.reset()
