###s3  front end  commands and ST33 migration tools

This S3  CLIs  may used with any compatible S3 storage: Amazon, Scality, Minio, etc..
It is built on top of the AWS golang SDK version 1. 

####  Installation

~~~~
install and configure  go 1.10+

cd $GOPATH
git pull https://github.com/PaulMatencio/s3

**Scality S3 frontend commands**
    cd $GOPATH/github/s3/sc
    make deps
    make install

**Scality commands for managing ACL** 
    cd $GOPATH/github/s3/acl 
    make install


**Command for using Scality IAM**
    cd  $GOPATH/github/s3/iam
    make install 

**st33 to Scality  S3 migration tools**
    cd  $GOPATH/github/s3/st33
    make install 
~~~~

####Configuration 

sc will look into these 3 locations for a configuration file name config.yaml

`./config.yaml`

`~/.sc/config.yaml`

`/etc/sc/config.xaml`

if a config.yaml file could not be found, sc will use the aws shared configuration 
files that are located in the .aws folder of the home directory. It is recommended to use the 
config.yaml file of the command line, however you can also configure the credential and
config files created by aws utility. 

***Example of a configuration file***
~~~~
s3:
  url: http://10.xx.xx.xx
  region: us-east-1
iam:
  url: http://10.xx.xx.xx:8600
  region: u-east-1
credential:
  access_key_id: myAccessKey
  secret_access_key: myVerySecretKey
logging:
  log_level: 3
  output: terminal   
meta:
  extension: md

~~~~

#### Usage:
  
  **sc [command]**

#####Available Commands:

~~~~
Available Commands:
  copyObj      Command to copy an object from one bucket to another
  delBucket    Command to delete a bucket
  delObj       Command to delete an object
  delObjs      Command to delete multiple objects  concurrently
  fgetObj      Command to download an objet from a given bucket to a file
  fputObj      Command to upload a given file to a bucket
  getBucketPol Command to get policies of a bucket
  getObj       Command to fetch an object from a given bucket
  getObjs      Command to download concurrently nultiple  objects and their metadata to a given directory
  headObj      Command to  verify if a given object exist and display the object metadata
  headObjs     Command to retieve some of the metadata of specific or every object in the bucket
  help         Help about any command
  lockObject   Command to lock an object
  lsBucket     Command to list all your buckets
  lsObjs       Command to list multiple objects of a given bucket
  mkBucket     Command to create a bucket
  putObjs      Command to upload multiple objects and their user metadata from a given directory to a bucket
  rmBucket     Command to delete a bucket
  rmObj        Command to delete an object
  rmObjs       Command to delete multiple objects  concurrently
  statBucket   Command to verify if a  given bucket exist
  statObj      Command to  verify if a given object exist and display the object metadata
  statObjs     Command to retieve some of the metadata of specific or every object in the bucket

Flags:
  -C, --autoCompletion   generate bash auto completion
  -c, --config string    sc config file; default $HOME/.sc/config.yaml
  -h, --help             help for sc
  -l, --loglevel int     Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)
  -P, --profiling int    display memory usage every P seconds
  -v, --verbose          verbose output

Use "sc [command] --help" for more information about a command.

    
    
~~~~
***Use "sc [command] --help" for more information about a command.***

####Bash autocompletion script

Use the flag -C along with any command to generate a bash auto completion script.
 Copy the generated _sc_bash_completion_ script to _/etc/bash_completion.d_ or 
 just add "source sc_bash_completion" to your .basrc file to active the bash autocompletion
 for the sc CLI



####  st33 to Scality  S3 migration tools

####Usage:   st33 [command]
~~~
st33 to S3 migration tools

Usage:
  st33 [command]

Available Commands:
  chkFiles    Command to check if all the  Tiff images and Blobs of a given st33 data file have been written to a folder
  chkS3       Command to check if all the  Tiff images and Blobs of a given st33 data file have been migrated to a S3 bucket
  help        Help about any command
  lsCtrl      Command to list a control file
  toFiles     Command to extract an ST33 file containing Tiff images and Blobs to Files
  toS3        Command to extract ST33 file containing Tiff Images and Blob and upload to S3

Flags:
  -C, --autoCompletion   generate bash auto completion
  -c, --config string    sc config file; default $HOME/.sc/config.yaml
  -h, --help             help for st33
  -l, --loglevel int     Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)
  -P, --profiling int    display memory usage every P seconds
  -t, --test             test mode
  -v, --verbose          verbose output

Use "st33 [command] --help" for more information about a command.


~~~

####  Scality S3 ACL commands
#### Usage:  acl [command]
~~~
Scality S3 commands for managing   buckets and objects ACL

Usage:
  acl [command]

Available Commands:
  getBucket   Command to get Bucket ACL
  getObj      Command to get Object ACL
  help        Help about any command
  putObj      Command to put Object ACL
  putBucket   Command to put Bucket ACL

Flags:
  -C, --autoCompletion   generate bash auto completion
  -c, --config string    sc config file; default $HOME/.sc/config.yaml
  -h, --help             help for acl
  -l, --loglevel int     Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)
  -P, --profiling int    display memory usage every P seconds
  -v, --verbose          verbose output

Use "acl [command] --help" for more information about a command.

~~~

~~~
Command for managing Scality IAM

Usage:
  iam [command]

Available Commands:
  getIAMPolicy Command to get scality IAM policy
  help         Help about any command

Flags:
  -C, --autoCompletion   generate bash auto completion
  -c, --config string    sc config file; default $HOME/.sc/config.yaml
  -h, --help             help for iam
  -l, --loglevel int     Output level of logs (1: error, 2: Warning, 3: Info , 4 Trace, 5 Debug)
  -P, --profiling int    display memory usage every P seconds
  -v, --verbose          verbose output

Use "iam [command] --help" for more information about a command.


~~~