# civis-jobs-public

Welcome to `civis-jobs-public`! Over the course of the Bernie 2020 Presidential Campaign, the data engineering team did a significant amount of development within the Civis Platform to manage our data processes. While we cannot open source our entire ETL and pipeline infrastructure, we can offer some selected utilities and projects that may be of use for the growing number of Civis Platform users in the progressive space. 

Note that due to database and platform dependencies this code will not run "out of the box", feel free to use and modify the code as needed for your usecase. The code will not be actively maintained, but we will do our best to respond to questions that are submitted to the repo as an issue. 

## Contents

### [A local development environment](https://github.com/Bernie-2020/civis-jobs-public/tree/master/.devcontainer)  

One pitfall we found while developing in Civis Platform was added difficulty in testing code locally since the platform manages database dependencies, credentials, and other parameters in each container. This Dockerfile manages those basic dependencies and streamlines the process to set up a local container environment. Any further parameters can be added to the environment with the command `export VARNAME=<value>`.

### Utilities

### Matching Pipeline

### Civis Audit

### NGPAN Pipelines


## Authors 
While there were many contributors to this repository over time, the leaders and members of the data engineering team were the primary contributors. Thank you for your hard work.

Michael Futch, Director of Analytics\
Gustavo Sanchez, Director of Data Engineering\
Jason Prado, Consulting Engineer\
Daniel Bravo, Data Science Engineer\
Isaac Flores, Data Science Engineer\
Claire Herdeman, Data Science Engineer\
Nico Marchio, Data Science Engineer\
Ross Greenwood, Data Science Engineer\
KJ Deyett, Data Science Engineer
