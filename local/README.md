# Preparing the Local Environment
1. Create a directory to use as a mount point for redis and minio. In the example I use the path `~/dataengineering`.
2. Update your Docker File Sharing permissions to enable reading/writing to the mount point. (Docker > Preferences > Resources > File Sharing) if you are running on Docker Desktop 


## Running the Local Environment

In order to use the `redis` hostname locally, you need to add the entry to your `/etc/hosts` - `sudo vim /etc/hosts`

Then add the following
```
127.0.0.1 redis
127.0.0.1 minio
```

## Spin up the Docker Environment
From this directory `spark-inception/local`, do the following from the `terminal`.

1. Ensure Docker is running : The `docker version` command will return client / server information if things are running.
2. Ensure you have the external network `mde` setup (bridge network). `docker network create mde`. You'll see the network id. You can also run `docker network ls` as well to confirm.
3. Execute Docker Compose: `docker compose up`

## Handling Exceptions and Docker Permissions
```
Error response from daemon: Mounts denied: 
The path /minio/data is not shared from the host and is not known to Docker.
You can configure shared paths from Docker -> Preferences... -> Resources -> File Sharing.
See https://docs.docker.com/desktop/mac for more info.
```