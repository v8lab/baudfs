# Baudfs

## Overview

Baudfs is a unified distributed filesystem for small files and large files. 

* key-file data model, i.e., namespaceless

* hierachical namespace

* unlimited scalability of both metadata and data

* strong consistency

Baudfs has been built and deployed in production since 2013. 

## Architecture

metadata partition: inode range

data partition: two storage engines: tiny file chunks, extents; one replication protocol

Baudfs consists of several components:

* the cluster master. single raft replication

* metanode. multi-raft replication, a meta partition (inode range) per raft

* datanode. de-clustering of data partitions, replicated via a consistent replication protocol


## Interfaces

- Go SDK
- Java SDK
- FUSE
- NFS


## Usage

* Image Store

based on the namespace-less key-file interface, nginx integration

* Object Store

integration with minio

* Big Data

integration with Hadoop

* Container Persistent Volumes

integration with Kubernetes


