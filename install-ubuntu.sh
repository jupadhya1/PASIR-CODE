#!/bin/bash
set -e
INSTALL_DEST="/opt/pasir-classr"
DAEMON_USER="classrapi"

sudo -s apt-get update
sudo -s apt-get -y install git python-dev python-pip libblas-dev liblapack-dev libatlas-base-dev gfortran

# core dependencies
sudo -s pip install pandas==0.13.1 
sudo -s pip install SciPy==0.9
sudo -s pip install Flask==0.10.1 
sudo -s pip install Flask-JSONRPC==0.3.1
sudo -s pip install Flask-HTTPAuth==3.1.2 
sudo -s pip install sklearn
sudo -s pip install --pre xgboost==0.4a30
sudo -s pip install requests==2.2.1

# dependencies for enabling IBM DB2 connection
sudo -s apt-get -y install openjdk-7-jre
sudo -s pip install JPype1==0.6.1 
sudo -s pip install JayDeBeApi==0.2.0

# compile paragrah vectors
cd pv
make
cd ..

# copy all stuff under /opt/pasir-classr
# create daemon user, chown program
sudo cp -r ../pasir-classr $INSTALL_DEST
sudo useradd -r -s /bin/false classrapi
sudo chown -R $DAEMON_USER $INSTALL_DEST

# onfigure init.d script, copy and set up
sudo chown root:root init-ubuntu.sh
sudo chmod +x init-ubuntu.sh
sudo cp init-ubuntu.sh /etc/init.d/pasir-classr

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
echo "Command to follow the log  : tail -f -n 25 $INSTALL_DEST/logs/classr.log"
echo "*************************************************************************"
echo "PASIR/Classr API installation finished."
