PROJECT (dnf-plugins-emerge NONE)
CMAKE_MINIMUM_REQUIRED (VERSION 2.4)

EXECUTE_PROCESS(COMMAND python3 -c "from sys import stdout; from distutils import sysconfig; stdout.write(sysconfig.get_python_lib())" OUTPUT_VARIABLE PYTHON_INSTALL_DIR)
EXECUTE_PROCESS(COMMAND python3 -c "import sys; sys.stdout.write('%s.%s' % (sys.version_info.major, sys.version_info.minor))" OUTPUT_VARIABLE PYTHON_MAJOR_DOT_MINOR_VERSION)
MESSAGE(STATUS "Python install dir is ${PYTHON_INSTALL_DIR}")

INSTALL (FILES emerge.py DESTINATION ${PYTHON_INSTALL_DIR}/dnf-plugins)
