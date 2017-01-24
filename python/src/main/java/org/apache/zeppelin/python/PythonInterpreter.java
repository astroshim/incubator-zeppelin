/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*  http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.zeppelin.python;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.ServerSocket;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.commons.io.IOUtils;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterHookRegistry.HookType;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.interpreter.util.InterpreterOutputStream;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import py4j.GatewayServer;

/**
 * Python interpreter for Zeppelin.
 */
public class PythonInterpreter extends Interpreter implements ExecuteResultHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PythonInterpreter.class);

  //public static final String BOOTSTRAP_PY = "/bootstrap.py";
  //public static final String BOOTSTRAP_INPUT_PY = "/bootstrap_input.py";
  public static final String ZEPPELIN_PYTHON = "zeppelin.python";
  public static final String DEFAULT_ZEPPELIN_PYTHON = "python";
  public static final String MAX_RESULT = "zeppelin.python.maxResult";

  private Boolean py4JisInstalled = false;
  private InterpreterContext context;
  private Pattern errorInLastLine = Pattern.compile(".*(Error|Exception): .*$");
  private String pythonPath;
  private int maxResult;

  private String pythonCommand = DEFAULT_ZEPPELIN_PYTHON;

  private GatewayServer gatewayServer;
  private DefaultExecutor executor;
  private int port;
  private InterpreterOutputStream outputStream;
  private BufferedWriter ins;
  private PipedInputStream in;
  private ByteArrayOutputStream input;
  private String scriptPath;
  boolean pythonscriptRunning = false;
  private static final int MAX_TIMEOUT_SEC = 10;


  public PythonInterpreter(Properties property) {
    super(property);
    try {
      File scriptFile = File.createTempFile("zeppelin_python-", ".py");
      scriptPath = scriptFile.getAbsolutePath();
    } catch (IOException e) {
      throw new InterpreterException(e);
    }
  }

  private void createPythonScript() {
    ClassLoader classLoader = getClass().getClassLoader();
    File out = new File(scriptPath);

    if (out.exists() && out.isDirectory()) {
      throw new InterpreterException("Can't create python script " + out.getAbsolutePath());
    }

    try {
      FileOutputStream outStream = new FileOutputStream(out);
      IOUtils.copy(
        classLoader.getResourceAsStream("python/zeppelin_python.py"),
        outStream);
      outStream.close();
    } catch (IOException e) {
      throw new InterpreterException(e);
    }

    logger.info("File {} created", scriptPath);
  }

  private void createGatewayServerAndStartScript() {
    // create python script
    createPythonScript();

    port = findRandomOpenPortOnAllLocalInterfaces();

    gatewayServer = new GatewayServer(this, port);
    gatewayServer.start();

    // Run python shell
    //CommandLine cmd = CommandLine.parse(getProperty("zeppelin.pyspark.python"));

    CommandLine cmd = CommandLine.parse("python");
    //CommandLine cmd = CommandLine.parse(getPythonCommand());
    cmd.addArgument(scriptPath, false);
    cmd.addArgument(Integer.toString(port), false);
    executor = new DefaultExecutor();
    outputStream = new InterpreterOutputStream(logger);
    PipedOutputStream ps = new PipedOutputStream();
    in = null;
    try {
      in = new PipedInputStream(ps);
    } catch (IOException e1) {
      throw new InterpreterException(e1);
    }
    ins = new BufferedWriter(new OutputStreamWriter(ps));

    input = new ByteArrayOutputStream();

    PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream, outputStream, in);
    executor.setStreamHandler(streamHandler);
    executor.setWatchdog(new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT));

    try {
      // astro should pass env
      Map env = EnvironmentUtils.getProcEnvironment();

      /*
      logger.info("astro env python ==> {}", env);
      for (Object key : env.keySet()) {
        logger.info("*** astro env python ==> {} : {}", key, env.get(key));
      }
      */
      logger.info("astro run python 1==> {}", getPythonCommand());

      //env.put("PYTHONPATH", "/Users/shim/miniconda2/envs/tensorflow/bin/python");
      env.put("PATH", getPythonCommand());

      executor.execute(cmd, null, this);
      logger.info("astro run python 2==> {}", getPythonCommand());
      pythonscriptRunning = true;
    } catch (IOException e) {
      throw new InterpreterException(e);
    }

    logger.info("astro run python 3==> {}", getPythonCommand());
    try {
      input.write("import sys, getopt\n".getBytes());
      logger.info("astro run python 4==> {}", getPythonCommand());
      ins.flush();
      logger.info("astro run python 5==> {}", getPythonCommand());
    } catch (IOException e) {
      throw new InterpreterException(e);
    }
  }

  @Override
  public void open() {
    logger.info("astro Got open signal!");
    pythonscriptRunning = false;

    // Add matplotlib display hook
    InterpreterGroup intpGroup = getInterpreterGroup();
    if (intpGroup != null && intpGroup.getInterpreterHookRegistry() != null) {
      registerHook(HookType.POST_EXEC_DEV, "z._displayhook()");
    }

    // Add matplotlib display hook
    createGatewayServerAndStartScript();
  }

  @Override
  public void close() {
    logger.info("astro Got close signal!");

    pythonscriptRunning = false;

    executor.getWatchdog().destroyProcess();
    new File(scriptPath).delete();
    gatewayServer.shutdown();
  }

  PythonInterpretRequest pythonInterpretRequest = null;

  /**
   *
   */
  public class PythonInterpretRequest {
    public String statements;
    public String jobGroup;

    public PythonInterpretRequest(String statements, String jobGroup) {
      this.statements = statements;
      this.jobGroup = jobGroup;
    }

    public String statements() {
      return statements;
    }

    public String jobGroup() {
      return jobGroup;
    }
  }

  Integer statementSetNotifier = new Integer(0);

  public PythonInterpretRequest getStatements() {
    synchronized (statementSetNotifier) {

      logger.info("statementSetNotifier : {}", statementSetNotifier);
      while (pythonInterpretRequest == null) {
        try {
          //logger.info("statementSetNotifier wating... : {}", statementSetNotifier);

          statementSetNotifier.wait(1000);
        } catch (InterruptedException e) {
        }
      }
      PythonInterpretRequest req = pythonInterpretRequest;
      pythonInterpretRequest = null;

      logger.info("statementSetNotifier retrun req : {}", req);
      return req;
    }
  }

  String statementOutput = null;
  boolean statementError = false;
  Integer statementFinishedNotifier = new Integer(0);

  public void setStatementsFinished(String out, boolean error) {
    logger.info("set finished called 1. out : {}, {}, ", out, error, statementFinishedNotifier);

    synchronized (statementFinishedNotifier) {
      statementOutput = out;
      statementError = error;

      logger.info("set finished called 2. out : {}, {}", out, error);
      statementFinishedNotifier.notify();
    }
  }

  boolean pythonScriptInitialized = false;
  Integer pythonScriptInitializeNotifier = new Integer(0);

  public void onPythonScriptInitialized() {
    synchronized (pythonScriptInitializeNotifier) {
      pythonScriptInitialized = true;
      pythonScriptInitializeNotifier.notifyAll();
    }
  }

  public void appendOutput(String message) throws IOException {
    logger.info("appenOutput =====>{}", message);
    outputStream.getInterpreterOutput().write(message);
  }

  /////////////////////////////////////////////

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {
    logger.info("astro interpreter 1==> {}", cmd);
    if (cmd == null || cmd.isEmpty()) {
      return new InterpreterResult(Code.SUCCESS, "");
    }

    logger.info("astro interpreter 2==> {}", cmd);
    this.context = contextInterpreter;

    logger.info("astro interpreter 3 pythonscriptRunning ==> {}", pythonscriptRunning);
    if (!pythonscriptRunning) {
      return new InterpreterResult(Code.ERROR, "python process not running"
        + outputStream.toString());
    }

    outputStream.setInterpreterOutput(context.out);

    logger.info("astro interpreter 4 pythonScriptInitialized ==> {}", pythonScriptInitialized);

    synchronized (pythonScriptInitializeNotifier) {
      long startTime = System.currentTimeMillis();
      while (pythonScriptInitialized == false
        && pythonscriptRunning
        && System.currentTimeMillis() - startTime < MAX_TIMEOUT_SEC * 1000) {
        try {
          pythonScriptInitializeNotifier.wait(1000);
        } catch (InterruptedException e) {
        }
      }
    }
    logger.info("astro interpreter 5==> {}", cmd);

    List<InterpreterResultMessage> errorMessage;
    try {
      context.out.flush();
      errorMessage = context.out.toInterpreterResultMessage();
    } catch (IOException e) {
      throw new InterpreterException(e);
    }

    if (pythonscriptRunning == false) {
      // python script failed to initialize and terminated
      errorMessage.add(new InterpreterResultMessage(
        InterpreterResult.Type.TEXT, "failed to start python"));
      return new InterpreterResult(Code.ERROR, errorMessage);
    }
    if (pythonScriptInitialized == false) {
      // timeout. didn't get initialized message
      errorMessage.add(new InterpreterResultMessage(
        InterpreterResult.Type.TEXT, "python is not responding"));
      return new InterpreterResult(Code.ERROR, errorMessage);
    }

    pythonInterpretRequest = new PythonInterpretRequest(cmd, null);
    statementOutput = null;

    synchronized (statementSetNotifier) {
      statementSetNotifier.notify();
    }

    synchronized (statementFinishedNotifier) {
      while (statementOutput == null) {
        try {
          statementFinishedNotifier.wait(1000);
        } catch (InterruptedException e) {
        }
      }
    }

    if (statementError) {
      return new InterpreterResult(Code.ERROR, statementOutput);
    } else {

      try {
        context.out.flush();
      } catch (IOException e) {
        throw new InterpreterException(e);
      }

      return new InterpreterResult(Code.SUCCESS);
    }
  }

  /**
   * Checks if there is a syntax error or an exception
   *
   * @param output Python interpreter output
   * @return true if syntax error or exception has happened
   */
  private boolean pythonErrorIn(String output) {
    boolean isError = false;
    String[] outputMultiline = output.split("\n");
    Matcher errorMatcher;
    for (String row : outputMultiline) {
      errorMatcher = errorInLastLine.matcher(row);
      if (errorMatcher.find() == true) {
        isError = true;
        break;
      }
    }
    return isError;
  }

  @Override
  public void cancel(InterpreterContext context) {
  }

  @Override
  public FormType getFormType() {
    return FormType.NATIVE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetFIFOScheduler(
        PythonInterpreter.class.getName() + this.hashCode());
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor) {
    return null;
  }

  public void setPythonPath(String pythonPath) {
    this.pythonPath = pythonPath;
  }

  public void setPythonCommand(String cmd) {
    logger.info("astro setPythonCommand => {}", cmd);
    pythonCommand = cmd;
  }

  public String getPythonCommand() {
    return pythonCommand;
  }

  private Job getRunningJob(String paragraphId) {
    Job foundJob = null;
    Collection<Job> jobsRunning = getScheduler().getJobsRunning();
    for (Job job : jobsRunning) {
      if (job.getId().equals(paragraphId)) {
        foundJob = job;
        break;
      }
    }
    return foundJob;
  }

  public GUI getGui() {
    return context.getGui();
  }

  public Integer getPy4jPort() {
    return port;
  }

  private int findRandomOpenPortOnAllLocalInterfaces() {
    Integer port = -1;
    try (ServerSocket socket = new ServerSocket(0);) {
      port = socket.getLocalPort();
      socket.close();
    } catch (IOException e) {
      LOG.error("Can't find an open port", e);
    }
    return port;
  }

  public int getMaxResult() {
    return maxResult;
  }

  @Override
  public void onProcessComplete(int exitValue) {
    pythonscriptRunning = false;
    logger.info("python process terminated. exit code " + exitValue);
  }

  @Override
  public void onProcessFailed(ExecuteException e) {
    pythonscriptRunning = false;
    logger.error("python process failed", e);
  }
}
