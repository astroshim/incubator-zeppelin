# ZeppelinCI

ZeppelinCI is a build system for Apache Zeppelin.

ZeppelinCI can 
* classloading test at the spark cluster in actual environment.
* test on the various Operation System environments and JDK.
* test on the various spark cluster environments.
* run parallel environment build.


### Requirements
* Jenkins
* Docker
* make


### Structure
![image](https://cloud.githubusercontent.com/assets/8110426/11679999/a07fef90-9e98-11e5-93a1-2ca8084e4040.png)
* ZeppelinCI is using `Docker` to run one or more build jobs in parallel at the same time.
* On container, `ZeppelinCI-Buildstep` controls the starting and stopping of Backends and Zeppelin for each version.
* Build is possible to operate more lightly by sharing `Spark Binary` through the Docker Volume.

### Getting Started
As follows, Zeppelin CI provide two command to `docker image` creation and `docker container` running.
It also has three items for supporting an interpreter.
```
]# make

  Several choices : 

   make build     to build all docker image.
   make run       to run all docker container.

   example) 
    ]# make build type=zeppelin item=spark_standalone 

  type : 

   backend        jobs for backends.
   zeppelin       jobs for zeppelin.

   sub item : 
   examples) 
    *              all items in the build system will be ran.
    spark_*        all spark cluster will be builded or ran.

    ]# make run type=backend item=spark_standalone
    ]# make run type=zeppelin item=spark_standalone REPO=[your repository url] BRANCH=[your branch]
```

Supports currently items are :
```
 > spark_standalone
 > spark_mesos
 > spark_yarn
```

To make docker images:
```
]# make build type=[backend|zeppelin] item=[spark_standalone|spark_mesos|spark_yarn]
```

To run docker containers:
```
]# make run type=[backend|zeppelin] item=[spark_standalone|spark_mesos|spark_yarn] \
   REPO=[your repository-url] BRANCH=[your branch]
```
* REPO & BRANCH : this parameter write your repository and branch that requested the `PR`.
* In Jenkins, Supports the environment variable about `REPO & BRANCH` of requested the `PR`.


### ScreenShots
* When created PR or push.
![image](https://cloud.githubusercontent.com/assets/8110426/11338319/bc717494-9236-11e5-876d-b219248c1f1f.png)

* When completed build.
![image](https://cloud.githubusercontent.com/assets/8110426/11338683/dccfcefa-9238-11e5-9477-387fbcf0e184.png)

---

## How to add build-item of Zeppelin-CI
> The current structure,
In addition to 'spark mesos, spark standalone, spark yarn', 
Apply the following steps In order to add new item.

### 1. Backend new item
- List of subdirectories
```
zeppelin-ci/build/backends/
F├── Makefile
D├── new_item
D├── spark_mesos
D├── spark_standalone
D└── spark_yarn
```

- Creating a directory : To copy 'zeppelin-ci/build/backends/new_item'.
```
]# cd zeppelin-ci/build/backends
]# cp -rf new_item  [your new_item]
```

- Modify Dockerfile
```
* Let's modify Dockerfile in a [new item] directory.
* To add the settings related to [new item] at the following comment.
# ---------------------------------
# install new item
# ---------------------------------
#
# - insert new item scripts.
# 
# - open port for new item
# EXPOSE xxxx
#
# ---------------------------------
```

- Modify bootstrap.sh
```
* Let's modify bootstrap.sh in a [new item] directory.
* To add the settings related to [new item] at the following 1) and 2) comment.

1) Starting or Setting new_item
   > ex)
      mesos-master --ip=0.0.0.0 --work_dir=/var/lib/mesos & > /dev/null
      mesos-slave --master=0.0.0.0:5050 --launcher=posix & > /dev/null

2) Setting SPARK_HOME for new_item
   > When referring to a SPARK_HOME at [new item]
   > ex) 
      echo "export MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so" >> $SPARK_HOME/conf/spark-env.sh
```

- Modify Makefile
```
]# cd zeppelin-ci/build/backends
]# vi Makefile
```
```
* There are the following notes at the bottom in the Makefile.
* To change the your item name to "new_item".
* 'Docker' associated commands and options insert 'Common' part of Guide into elif syntax.
  Anything else, to add your [new item] port.
 
    ...
        \
    elif [ $(type) = "new_item" ]; then\
        \
        echo "Please set here backend new_item."; \
        \
    else\
        echo "not found to run backend."; \
    fi

    # ----------------------------------------------------
    # - New_item Adding Guide
    # ----------------------------------------------------
    # * Common :
    #
    #   docker run -id \
    #   -v $(REPOSHARE_PATH):/reposhare \
    #   -e BUILD_TYPE=$(type) \
    #   -e ZCI_ENV=$(ZCI_ENV) \
    #   -e CONT_NAME=$(name) \
    #   --name $(name) \
    #   -h sparkmaster \
    #   -p $(GET_RND_PORT):$(ZEPPELIN_PORT) \
    #   -p $(GET_RND_PORT):$(ZEPPELIN_WEBSOCKET_PORT) \
    #   ...
    #   $(type):$(IMAGE_VERSION) /bin/bash; \
    #
    # ----------------------------------------------------
    # * Do it yourself setting new_item : Ports
    #
    #   -p $(GET_RND_PORT):$(NEW_ITEM_PORT) \
    #   -p $(GET_RND_PORT):$(...) \
    #
    # ----------------------------------------------------

   ...
```


### 2. Zeppelin new item
- List of subdirectories
```
zeppelin-ci/build/zeppelin/os/centos/
F├── build.sh
F├── Makefile
D├── new_item
D├── spark_mesos
D├── spark_standalone
D└── spark_yarn
```

- Creating a directory : To copy 'zeppelin-ci/build/zeppelin/os/centos/new_item'
```
]# cd zeppelin-ci/build/zeppelin/os/centos
]# cp -rf new_item  [your new_item]
```

- Modify Dockerfile
```
* In general, since most of the settings are common, if you have the necessary settings, you want to add.
# ---------------------------------
# install new item
# ---------------------------------
#
# - insert new item scripts.
#
# ---------------------------------
```

- Modify zeppelin-env.sh
```
* There are the following notes at the bottom in the zeppelin-env.sh.
* To add the settings related to [new item] at the following comment.

## Zeppelin CI
##
# <- Insert new_item conf
# e.g.
# export MASTER="yarn-client"
# export HADOOP_CONF_DIR="/usr/share/spark/conf"
#
```

- Modify Makefile
```
]# cd zeppelin-ci/build/zeppelin/os/centos
]# vi Makefile
```
```
* There are the following notes at the bottom in the Makefile.
* To change the your item name to "new_item".
* 'Docker' associated command/option is all the same at the elif syntax, there is no other change.

    ...
    elif [ $(type) = "new_item" ]; then\
        \
        echo "Please set here zeppelin new_item."; \
        \
        docker run --rm -i \
        -v $(REPOSHARE_PATH):/reposhare \
        -e BRANCH=$(BRANCH) \
        -e REPO=$(REPO) \
        -e ZCI_ENV=$(ZCI_ENV) \
        -e BUILD_TYPE=$(type) \
        -e CONT_NAME=$(BACKEND_CONTAINER_NAME) \
        -p $(GET_RND_PORT):$(ZEPPELIN_PORT) \
        -p $(GET_RND_PORT):$(ZEPPELIN_WEBSOCKET_PORT) \
        --link $(BACKEND_CONTAINER_NAME):$(NICKNAME) \
        zeppelin-$(type):$(IMAGE_VERSION) /bin/bash; \
        \
    else\
    ...
```

### 3. Package download
- List of subdirectories
```
zeppelin-ci/build/localrepo/
D├── bin
F│   ├── get_build_dat.sh         #  pacakge download into localrepo/dat
F│   ├── get_confirm_build_dat.sh # To copy the package to directory of the [new item]
D└── dat
```
- The necessary Package of [new item] in order to quick 'build-up' is downloaded to path of localrepo in advance.

```
* Download Script : zeppelin-ci/build/localrepo/bin/get_build_dat.sh
* To Copy & Paste & Modify refer the scripts below it.
* The download url of the [new item] package modify the wget part.

...
if [ $item = "spark_yarn" ]; then      #<- Modify your item name
    echo "@ Download Hadoop : $REPO_HOME"
    HADOOP_BIN=hadoop-$HADOOP_VERSION.tar.gz
    if [ ! -f $REPO_HOME/$HADOOP_BIN ]; then
        echo " - Doesn't exist -> Downloading hadoop : $REPO_HOME/$HADOOP_BIN"
        echo ""
        wget -P $REPO_HOME https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/$HADOOP_BIN
    else
        echo " - Already exist : $HADOOP_BIN"
    fi
fi
...
```
```
* Download Script : zeppelin-ci/build/localrepo/bin/get_confirm_build_dat.sh
* The scripts below it is to copy downloaded packages of localrepo to your [new item] directory.
  ( Modify item are the same as above )

...
if [ $type = "spark_yarn" ]; then      #<- Modify your item name
    echo "@ Confirm hadoop-$HADOOP_VERSION binary"
    HADOOP_BIN=hadoop-$HADOOP_VERSION.tar.gz
    if [ ! -f $REPO_HOME/$HADOOP_BIN ]; then
        echo " - Doesn't exist hadoop : $REPO_HOME/$HADOOP_BIN"
        echo ""
        wget -P $REPO_HOME https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/$HADOOP_BIN
    fi
fi
cp -f $REPO_HOME/$HADOOP_BIN $BUILD_PATH/hadoop.tar.gz
...
```

### 4. Precautions
- The [new item] name of the 'backends' and 'zeppelin' have to be the same.
