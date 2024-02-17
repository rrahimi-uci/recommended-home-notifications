#!/bin/bash

set -x
set -e


export PYTHONPATH=${PYTHONPATH}:/usr/local/lib/python3.6/site-packages
echo $PYTHONPATH

sudo python3 -m ensurepip
echo $`python3.6 -m pip --version`
echo $`which pip-3.6`


sudo /usr/bin/python3 -m pip install -U pip

sudo cp /usr/local/bin/pip3.6 /usr/bin/
sudo cp /usr/local/bin/pip3 /usr/bin/

sudo /usr/bin/pip3.6 install boto3
sudo /usr/bin/pip3.6 install optimizely-sdk

unset PYTHONPATH

SourceBucket="rdc-recommended-notifications-$1"
aws s3 cp s3://${SourceBucket}/rdc-recommended-notifications.zip /mnt/rdc-recommended-notifications/

export PYTHONPATH=${PYTHONPATH}:/usr/local/lib/python3.6/site-packages
echo $PYTHONPATH

cd  /mnt/rdc-recommended-notifications/
unzip -o rdc-recommended-notifications.zip
