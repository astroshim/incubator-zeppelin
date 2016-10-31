package org.apache.zeppelin.eventhook;

import com.google.common.base.Preconditions;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Created by shim on 2016. 10. 30..
 */
public class FilterManager {
  FilterChain filterChain;
  private Logger logger = LoggerFactory.getLogger(FilterManager.class);

  public FilterManager(Target target){
    filterChain = new FilterChain();
    filterChain.setTarget(target);
  }
  public void setFilter(Filter filter){
    filterChain.addFilter(filter);
  }

  public void filterRequest(String request){
    filterChain.execute(request);
  }

  Map<String, URLClassLoader> myClassLoaders = new LinkedHashMap<String, URLClassLoader>();

  public void loadExtModule() throws MalformedURLException, InstantiationException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {
    loadExtModule("eventhook", "/Users/shim/zeppelin/eventhook/", "org.apache.zeppelin.eventhook.FilterManager", null, null) ;
  }

  @SuppressWarnings("rawtypes")
  private static Class [] getType(Object [] params) {
    if(params==null) return null;
    Class [] constType = new Class[params.length];
    for(int i=0; i<params.length;i++){
      if(params[i]==null) constType[i] = null;
      else constType[i] = params[i].getClass();
    }
    return constType;
  }

  /**
   *
   * @param moduleName : map의 key 이다. 사실 여기서는 별로 필요하지 않을듯
   * @param libPath : 해당 key의 jar가 있는 path.
   * @param className
   * @param methodName
   * @param params
   * @throws MalformedURLException
   * @throws InstantiationException
   * @throws InvocationTargetException
   * @throws NoSuchMethodException
   */
  public void loadExtModule(String moduleName, String libPath, String className, String methodName, Object [] params)
    throws MalformedURLException, InstantiationException, InvocationTargetException, NoSuchMethodException, IllegalAccessException {

    Preconditions.checkNotNull(libPath);

    logger.info("Load Libraries libPath = "+libPath);
    File files = new File(libPath);
    File [] jars = files.listFiles();
    URL [] urls = new URL[jars.length];

    Preconditions.checkNotNull(jars);
    for (int i = 0; i < jars.length; i++) {
      if (jars[i].isDirectory()) continue;
      if (jars[i].getName().startsWith(".")) continue;

      logger.info("  add "+jars[i].getAbsolutePath());
      urls[i] = jars[i].toURI().toURL();


      // get class name from jar.
      JarFile jarFile = null;
      try {
        jarFile = new JarFile(jars[i].getAbsoluteFile());
      } catch (IOException e) {
        e.printStackTrace();
      }
      Enumeration allEntries = jarFile.entries();
      while (allEntries.hasMoreElements()) {
        JarEntry entry = (JarEntry) allEntries.nextElement();
        String name = entry.getName();

        if (name.endsWith(".class")) {
          System.out.println("===> " + name);
          logger.info("===> " + name);
        }
      }

    }


    ClassLoader oldcl = Thread.currentThread().getContextClassLoader();
    URLClassLoader cl = new URLClassLoader(urls, oldcl);

    try {
      Thread.currentThread().setContextClassLoader(cl);
      myClassLoaders.put(moduleName, cl);

      Class cls = cl.loadClass(className);

      if (methodName != null) {
        Method m;
        Object inst;
        m = cls.getMethod(methodName, getType(params));
        inst = cls.newInstance();

        // logger.info("==> " + m);
        Object ret = m.invoke(inst, params);

        logger.info("invoke return value => " + ret);

      }
      else {
        logger.info("Using reflections to gather all UnitInstanceCollector classes");

// using goole Reflection library.
//        Reflections reflextions = new Reflections(new ConfigurationBuilder()
//          //.addScanners(new SubTypesScanner())
//          .filterInputsBy(new FilterBuilder().include("org.apache.zeppelin.eventhook.*"))
//          .addClassLoader(cl)
//          .setScanners(new SubTypesScanner())
//          .addUrls(ClasspathHelper.forClassLoader(cl)));
//
//        Set<Class<? extends Filter>> classes = reflextions.getSubTypesOf(Filter.class);
//        logger.info("Loaded {} Elise collector(s) in the ./extensions folder", classes.size());
//        for (Class c : classes) {
//          if (c == null) {
//            logger.info("Loaded class is null, I do not know what is happening !");
//          } else {
//            logger.info("filter class : " + c.toString());
///*
//
//            final Method onLoadMethod = c.getMethod("onLoad");
//            onLoadMethod.invoke(null);
//*/
//          }
//        }
//        //return classes;
//



/*

      // 모든 method 출력.
      Method[] mothods = cls.getMethods();
      for (Method m1 : mothods){
        try {
          logger.info("Method: " + m1.getName());

//	                    if (m1.getName().equals("myMethodTest")) {
//	                    	Object inst = cls.newInstance();
//	                        Object o = m1.invoke(inst, null);
//	                        logger.info("o is : " + o);
//	                    }
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
*/
    }

      /**
       *
       ///////////////////////////////////////////////////////////////
       // test1.
       // myMethodTest() 라는 method 를 호출한다.
       ///////////////////////////////////////////////////////////////
       //@SuppressWarnings("unchecked")
       Method m = cls.getMethod("myMethodTest", null);
       Object inst = cls.newInstance();
       logger.info("==> " + m);
       m.invoke(inst, null);

       ///////////////////////////////////////////////////////////////
       // test2.
       // myMethodTest(String param) 라는 method 를 호출한다.
       ///////////////////////////////////////////////////////////////
       m = cls.getMethod("myMethodTest", new Class[]{String.class});
       inst = cls.newInstance(); // <== This is the new line
       logger.info("==> " + m);
       m.invoke(inst, new Object[] { new String("GOOOOOOOOOOOOOOOOOOOOOOOD") });

       ///////////////////////////////////////////////////////////////
       // test3.
       // myMethodTest(String param) 라는 method 를 호출한다.
       ///////////////////////////////////////////////////////////////
       Object [] params = new Object[]{"abc"};
       m = cls.getMethod("myMethodTest", getType(params));
       inst = cls.newInstance(); // <== This is the new line
       logger.info("==> " + m);
       m.invoke(inst, params);
       */

      //return (Object) m.invoke(null, params);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      Thread.currentThread().setContextClassLoader(oldcl);
    }
  }

  public static void main(String args[]) {
    FilterManager filterManager = new FilterManager(new Target());

    try {
      try {
        filterManager.loadExtModule();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    }
  }

}
