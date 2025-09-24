# Utilities for batch download persistence in Redis

## Clean redis hmap from files on disk

Batch download is creating some archives on disk to be downloaded by the user. These archive files are kept during 2 weeks but the records in redis are not removed for display in the UI. Sometimes we want to purge the records that are linked to nonexistent files.

To do so, the script needs access to the filesystem to check if files are still existing. 

1. Build a pip compatible package:
```shell
poetry build
```
2. Copy the script to your user account on the server that has access to Redis and filesystem:
```shell
scp build/batch_download_utils-0.1.0.tar.gz user@server:
```
3. Connect to the server and install the pip package in your local environment:
```shell
ssh user@server
pip3 install batch_download_utils-0.1.0.tar.gz 
```
4. Launch the command `hmap-cleaner` providing redis-url and path to the files:
```shell
hmap-cleaner redis://my-redis-host:6379 /path/to/download/archive/files
```

## Migrate task related data model 

Tasks JSON structure is evolving along the Java models. To avoid any errors when accessing to task and their results it is necessary to migrate the data in Redis accordingly. The script is provided to modify the data structure of the objects in the redis hashmap.

To apply the script to the server, a SSH bridge can be used like this:
```shell
ssh -N -L localhost:6379:localhost:6379 user@server
```

Consequently, the remote redis instance will be accessible from the local machine:
```shell
redis-cli -h localhost
```

Running the script will apply changes on the server:
```shell
hmap-migrate redis://localhost:6379
```

You can change the main scripting steps for your needs by using the provided utility functions (you can add some if you can't find your need). 

It is not a full schema migration system with a list of migrations that could be rolled back, stacked (and so on) because the persistence of the tasks manager is going to be changed from Redis to PostgreSQL. Thus, the migrations will be made by datashare in java (with [liquibase](https://www.liquibase.com/)).


## Migrate batch search task result 

To use the latest data model of JSON tasks (batch search results) : 

```shell
#for sqlite
db-migrate-bs-result-task "jdbc:sqlite:file:/path/to/db/datashare.db"
```

```shell
#for postgresql
db-migrate-bs-result-task "jdbc:postgresql://postgres/datashare?user=admin&password=admin"
```

```shell
#for mysql
db-migrate-bs-result-task "jdbc:postgresql://database/datashare?user=admin&password=admin"
```