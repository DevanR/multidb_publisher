## Overview

This module allow the user to run mulitple queries on multiple databases simultaneously
and push the results to a target machine. An example of how the module may be used is
provided in main.py

## configs

This folder contains two files. database.local.yaml contains the access config for the
input databases. target.local.yaml contains the access config for the target machine.

## inputs

This folder contains the yaml configs for how data is published to the target machine. 
For this case, the target machine API allows for the publication of reports and catalogues.
Catalogues are divided into separate folders named by the database (which is reference in the
database.local.yaml file.

