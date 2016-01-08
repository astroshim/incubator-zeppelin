### Makefile by astroshim <hsshim@nflabs.com>
### This is a makefile for making docker images or running docker containers.

.PHONY: help env build run clean


# -----------------------------------------------------------------------------
# - Define Varialbes
# -----------------------------------------------------------------------------

BUILD_HOME=$(shell pwd)
LOCALREPO_BIN=$(BUILD_HOME)/build/localrepo/bin
LOCALREPO_DAT=/opt/localrepo

ZCI_ENV_FILE=.zci.env
USER_ZCI_ENV=$(ZCI_ENV_FILE).$(userhome)
ZCI_ENV=$(BUILD_HOME)/$(ZCI_ENV_FILE)
ZCI_YML=$(BUILD_HOME)/zci.yml

REPOSHARE_DIR=/tmp/build/reposhare

BUILD_DIR=/tmp/build/$(userhome)
ZEPPELIN_BUILD_DIR=$(BUILD_DIR)/zeppelin
INTERPRETER_BUILD_DIR=$(BUILD_DIR)/backends


# -----------------------------------------------------------------------------
# - Call Funtions
# -----------------------------------------------------------------------------

setup_comm = \
	mkdir -p $(REPOSHARE_DIR); \
	cp -f $(ZCI_ENV) $(REPOSHARE_DIR)/$(USER_ZCI_ENV)

setup_back = \
	$(call setup_comm); \
	mkdir -p $(INTERPRETER_BUILD_DIR); \
	rm -rf $(INTERPRETER_BUILD_DIR)/$(item); \
	cp -rf build/backends/$(item) $(INTERPRETER_BUILD_DIR); \
	cp -f build/backends/Makefile $(INTERPRETER_BUILD_DIR)

setup_zepp = \
	$(call setup_comm); \
	mkdir -p $(ZEPPELIN_BUILD_DIR)/os/centos; \
	rm -rf $(ZEPPELIN_BUILD_DIR)/os/centos/$(item); \
	cp -rf build/zeppelin/os/centos/$(item) $(ZEPPELIN_BUILD_DIR)/os/centos; \
	cp -f build/zeppelin/os/centos/Makefile $(ZEPPELIN_BUILD_DIR)/os/centos; \
	cp -f build/zeppelin/os/centos/build.sh $(ZEPPELIN_BUILD_DIR)/os/centos

env_job = \
	@source $(ZCI_ENV); \
	echo "* Image Version  : $$IMAGE_VERSION";\
	echo "* JDK Version    : $$JDK_VERSION";\
	echo "* Hadoop Version : $$HADOOP_VERSION";\
	echo "* Spark Version  : $$SPARK_VERSION";\

run_job =  \
	source $(ZCI_ENV); \
	for dir in $$(find $(1) -type d); do \
	  ( \
		cd $$dir ; \
		if [ -f Makefile ]; then \
			for t in $$dir/*; do \
			  (if [ -d $$t ]; then \
				if [[ `basename $$t` == $(3) ]]; then \
					cp -f $(BUILD_HOME)/build/buildstep.sh $$t; \
					echo ""; echo -n -e "@ Target Path : $$t\n - "; \
					make $2 -f Makefile \
						type=`basename $$t` \
						name=$(name) \
						REPO=$(REPO) \
						BRANCH=$(BRANCH) \
						BUILD_HOME=$(BUILD_HOME) \
						BUILD_PATH=$$t \
						ZCI_ENV=$(USER_ZCI_ENV) \
						REPOSHARE_PATH=$(REPOSHARE_DIR); \
					$(BUILD_HOME)/build/buildstep.sh putres $(REPOSHARE_DIR) $(name) $$?; \
			  	fi; \
			  fi; \
			); done \
		fi; \
	/bin/echo " "; \
	); done	


# -----------------------------------------------------------------------------
# - Build options
# -----------------------------------------------------------------------------

help:
	@echo
	@echo "  Several choices : "
	@echo
	@echo "   make build     to build all docker image."
	@echo "   make run       to run all docker container."
	@echo 
	@echo "   example) "
	@echo "    ]# make build type=backend item=spark_standalone "
	@echo "    ]# make build type=zeppelin item=spark_standalone "
	@echo 
	@echo "  type : "
	@echo 
	@echo "   backend        jobs for backends."
	@echo "   zeppelin       jobs for zeppelin."
	@echo 
	@echo "   sub item : "
	@echo "   examples) "
	@echo "    *              all items in the build system will be ran."
	@echo "    spark_*        all spark cluster will be builded or ran."
	@echo
	@echo "    ]# make run type=backend item=spark_standalone name=[Container Name]"
	@echo "    ]# make run type=zeppelin item=spark_standalone name=[Backend Container Name]"
	@echo "       REPO=[your repository url] BRANCH=[your branch]"
	@echo 

env :
	@echo "$(ZCI_ENV)" > .envfile
	@build/buildstep.sh envload $(ZCI_YML) $(ZCI_ENV)

build : env
	@if [ -z $(type) ]; then \
		echo "Type \"make help\" to run. please..";\
		exit 1; \
	fi; \
	if [ -z $(item) ]; then \
		echo "Type \"make help\" to run. please..";\
		exit 1; \
	fi; \
	echo "* Build Type     : $(type)"; echo ""; \
	\
	$(LOCALREPO_BIN)/get_build_dat.sh $(ZCI_ENV) $(LOCALREPO_DAT); \
	if [ "$(type)" = "backend" ]; then \
		$(call setup_back); \
		$(call run_job,$(INTERPRETER_BUILD_DIR),$@,$(item)) \
	elif [ "$(type)" = "zeppelin" ]; then \
		$(call setup_zepp); \
		$(call run_job,$(ZEPPELIN_BUILD_DIR),$@,$(item)) \
	else \
		echo "no type you want!"; \
		exit 1; \
	fi;

run : env
	@if [ -z $(type) ]; then \
		echo "Type \"make help\" to run. please..";\
		exit 1; \
	fi; \
	\
	if [ "$(type)" = "backend" ]; then \
		$(call setup_back); \
		$(call run_job,$(INTERPRETER_BUILD_DIR),$@,$(item)) \
	elif [ "$(type)" = "zeppelin" ]; then \
		$(call setup_zepp); \
		$(call run_job,$(ZEPPELIN_BUILD_DIR),$@,$(item)) \
	else \
		echo "no type you want!"; \
		exit 1; \
	fi;

clean :
	@rm -rf $(REPOSHARE_DIR)


# -----------------------------------------------------------------------------
# - End of File
# -----------------------------------------------------------------------------
