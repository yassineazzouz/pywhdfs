PyWHDFS
==================================

API and interactive command line interface for Web HDFS.

```bash
  $ pywhdfs -c prod

  ---------------------------------------------------
  ------------------- PYWEBHDFS 1.0.0 ------------------
  -- Welcome to WEB HDFS interactive python shell. --
  -- The WEB HDFS client is available as `CLIENT`. --
  --------------------- Enjoy ! ---------------------
  ---------------------------------------------------


  >>> CLIENT.list("/")
  [u'admin', u'data', u'group', u'solr', u'system', u'tmp', u'user']
```

Functionalities
--------

* Python Library for interacting with WebHDFS and HTTFS Rest API
* Support both secure (Kerberos,Token) and insecure clusters
* Supports HA cluster and handle namenode failover
* Supports HDFS federation with multiple nameservices and mount points.
* Json format clusters configuration. 
* Command line interface to interactively interact with WebHDFS API
  on a python shell.
* Support concurency on uploads and downloads.


Getting started
---------------

```bash
  $ easy_install pywhdfs
```


Configuration
---------------

PyWHDFS uses a json configuration file that define the connection parameters
for the different clusters.
A simple configuration file looks like:

```json
  {
    "clusters": [
      {
        "name": "prod",
        "auth_mechanism": "GSSAPI",
        "verify": false,
        "truststore": "trust/store/path.jks",
        "nameservices": [
          {
            "urls": ["http://first_namenode_url:50070" , "http://second_namenode_url:50070"],
            "mounts": ["/"]
          }
         ]
      }
    ]
  }
```

The configuration file is validated against a [ schema file ](pywhdfs/resources/config_schema.json)

The default location of configuration file is "~/.webhdfs.cfg" but can can be overwritten using
WEBHDFS_CONFIG environement variable.

USAGE
-------

The interactive python shell client is the easiest way to use pywhdfs, but you can also instanciate
the client manually :

```python 
 >>>import pywhdfs.client as pywhdfs
 >>>CLIENT = pywhdfs.WebHDFSClient(nameservices=[{'urls':[ "http://host1.hadoop.domain:50070" , "http://host2.hadoop.domain:50070"],'mounts':['/']}], auth_mechanism="GSSAPI", verify=False)
 >>>CLIENT.list("/")
```

The interacctive shell requires the connection parameters for the cluster to be setup in the configuration file,
and the cluster name needs to match the name you pass as argument. 

Contributing
------------

Feedback and Pull requests are very welcome!
