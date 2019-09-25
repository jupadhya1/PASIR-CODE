#!/bin/bash
set -e
INSTALL_DEST="/opt/pasir-classr"
DATA_DEST="/app/pasir-classr-data"
DAEMON_USER="classrapi"

ORIG_PWD=`pwd`

# install pre-req's for Python and its packages
sudo -s rpm --force -ivh http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
sudo -s yum check-update
sudo -s yum install -y blas-devel lapack-devel atlas-devel libgfortran

# install python 2.7
sudo -s yum install -y gcc
sudo -s yum groupinstall -y development
sudo -s yum install -y zlib-dev openssl-devel sqlite-devel bzip2-devel
cd /usr/src
sudo wget https://www.python.org/ftp/python/2.7.12/Python-2.7.12.tgz
sudo tar xzf Python-2.7.12.tgz
cd Python-2.7.12
sudo -s ./configure
sudo -s make altinstall

# install pip
sudo -s curl -O https://bootstrap.pypa.io/get-pip.py
sudo -s /usr/local/bin/python2.7 get-pip.py

# install Python core dependencies
sudo -s /usr/local/bin/pip2.7 install pandas==0.13.1 
sudo -s /usr/local/bin/pip2.7 install SciPy==0.9
sudo -s /usr/local/bin/pip2.7 install Flask==0.10.1 
sudo -s /usr/local/bin/pip2.7 install Flask-JSONRPC==0.3.1
sudo -s /usr/local/bin/pip2.7 install Flask-HTTPAuth==3.1.2 
sudo -s /usr/local/bin/pip2.7 install sklearn
sudo -s /usr/local/bin/pip2.7 install --pre xgboost==0.4a30
sudo -s /usr/local/bin/pip2.7 install requests==2.2.1

# dependencies for enabling IBM DB2 connection
sudo -s yum -y install java-1.7.1-ibm
sudo -s /usr/local/bin/pip2.7 install JPype1==0.6.1 
sudo -s /usr/local/bin/pip2.7 install JayDeBeApi==0.2.0

# compile paragrah vectors
cd $ORIG_PWD/pv
make
cd ..

# copy all stuff under /opt/pasir-classr
# create daemon user, chown program
sudo cp -r ../pasir-classr $INSTALL_DEST
sudo useradd -r -s /bin/false classrapi
sudo chown -R $DAEMON_USER:$DAEMON_USER $INSTALL_DEST

# configure init.d script, copy and set up
sudo chown root:$DAEMON_USER init-rhel.sh
sudo chmod +x init-rhel.sh
sudo cp init-rhel.sh /etc/init.d/pasir-classr

# configure data storage
sudo mkdir -p $DATA_DEST/logs
sudo mkdir -p $DATA_DEST/resources
sudo mkdir -p $DATA_DEST/work
sudo mkdir -p $DATA_DEST/tmp
sudo chown -R $DAEMON_USER:$DAEMON_USER $DATA_DEST
sudo chmod -R 775 $DATA_DEST

# print installation summary
echo ""
echo ""
echo ""
echo ""
echo ""
echo "*************************************************************************"
echo "Destination                : $INSTALL_DEST"
echo "Configs location           : cd $INSTALL_DEST/config && ls"
echo "Command to start the API   : sudo /etc/init.d/pasir-classr start"
echo "Command to follow the log  : tail -f -n 25 $DATA_DEST/logs/classr.log"
echo "*************************************************************************"
echo "PASIR/Classr API installation finished."