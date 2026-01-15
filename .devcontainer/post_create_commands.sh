#!/bin/bash

git config core.fileMode false
pip install --upgrade pip
pip install -r requirements.txt
sudo apt-get update && sudo apt-get install -y awscli
