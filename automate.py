#!/usr/bin/env python3
"""
Script to facilitate data transfers and image analysis using the Fiji software
"""

import os
import sys
import shutil
import paramiko
import re
import yaml
import getpass
# import hashlib
import tqdm
from datetime import datetime
from pathlib import Path


# Track any errors into 'ErrorsFound'
ErrorsFound = False

# -- Open log file for writing and append date/time stamp into file for a new entry
logfile = 'log.txt'
log = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), logfile), 'a')
log.write('\n----------------------------------------------- \n')
log.write('------------  '+datetime.now().strftime('%Y-%m-%d %H:%M:%S')+'  ------------ \n')
log.write('----------------------------------------------- \n')


# -- Read in details from settings file
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.yaml"), 'r') as yamlfile:
    matcher_info = yaml.safe_load(yamlfile)
    storage_username = matcher_info['storage_username']
    key_file = Path(matcher_info['key_file'])
    regex_matcher = matcher_info['regex_matcher']
    hiestorageDir = matcher_info['hiestorageDir']
    cleanedDir = matcher_info['cleanedDir']
    key_file = matcher_info['key_file']


# -- Configure HIE-storage SFTP setup
host = "hie-storage.intersect.org.au"
port = 22
try:
    key_passphrase = getpass.getpass(prompt="Please enter your key passphrase: ")
    k = paramiko.RSAKey.from_private_key_file(key_file, password=key_passphrase)
    transport = paramiko.Transport((host, port))
    transport.connect(username=storage_username, pkey=k)
    sftp = paramiko.SFTPClient.from_transport(transport)
except Exception as e:
    log.write("Problem sftp'ing to HIE-Storage. Error: %s", e)
    ErrorsFound = True
    sys.exit(1)


# -- Move any subfolders matching the standardised naming from the 'Cleaned Images' folder (local) to
# the 'Pre-Subtraction' folder on hie-storage
log.write('Searching for directories in "cleaned data" to be transferred to HIE-Storage \n')
for item in os.listdir(cleanedDir):
    item_full_path_local = os.path.join(cleanedDir, item)
    if os.path.isdir(item_full_path_local) and re.match(regex_matcher, item):
        # Get the subdirectories checksum value for later successful transfer check
        # local_checksum = hashlib.md5(open(os.path.join(cleanedDir, item), 'rb').read()).hexdigest()
        log.write('Match found - %s \n' % item)
        # Create the matching subdirectory on hie-storage
        try:
            item_full_path_remote = os.path.join(hiestorageDir, item)
        except IOError as e:
            errno, strerror = e.args
            print("I/O error({0}): {1}".format(errno, strerror))

        # Check if directory already exists on hie-storage and abort if so
        if sftp.stat(item_full_path_remote):
            log.write('This directory already exists on hie-storage - Aborting \n')
            ErrorsFound = True
            break
        # Otherwise proceed to copy/sftp the folder to hie-storage
        sftp.mkdir(item_full_path_remote)
        log.write('Transferring %s to HIE-Storage \n', item)
        for dirpath, dirnames, filenames in os.walk(item_full_path_local):
            file_counter = 0
            for filename in tqdm(filenames):
                if filename.endswith('.tif'):
                    try:
                        sftp.put(os.path.join(item_full_path_local, filename), os.path.join(item_full_path_remote+'/', filename))
                        file_counter += 1
                    except IOError as e:
                        errno, strerror = e.args
                        print("I/O error({0}): {1}".format(errno, strerror))

        log.write('%s directory (containing %s tif files) transferred to HIE-Storage\n' % (item, file_counter))

    # Move the subfolder to a local backup folder
    backupDir = os.path.join(matcher_info['backupDir'], item)
    for src_dir, dirs, files in os.walk(item_full_path_local):
        dst_dir = src_dir.replace(item_full_path_local, backupDir)
        if os.path.exists(backupDir):
            log.write('This directory already exists in the backups folder - Aborting \n')
            ErrorsFound = True
            break
        else:
            os.mkdir(backupDir)
        for file_ in files:
            src_file = os.path.join(item_full_path_local, file_)
            dst_file = os.path.join(backupDir, file_)
            shutil.move(src_file, dst_dir)
    log.write('%s directory moved to Backups folder\n', item)

# -- Wrap up first stage
if ErrorsFound:
    log.write('*** Unsuccessful run\n')
else:
    log.write('*** Completed successfully\n')


log.write('------------------    END    ------------------- \n')
log.write('\n')

