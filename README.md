### 项目目的
该项目对zookeeper客户端curator源码进行尽量全面的注释，方便大家学习zookeeper与curator。同时也是我个人学习成长的一个小过程。

### zookeeper与curator
zookeeper是非常重要的分布式协调框架，如果大家有分布式相关的开发工作，对zookeeper有一定了解会有所帮助。  
curator是最流行的Zookeeper客户端之一。zookeeper原生的客户端使用起来极其复杂，curator对其进行进行封装，提供了高层次的API框架和一些工具方法，
使得使用zookeeper更容易和可靠。此外，它还提供了常见问题的解决方案，如服务发现，反复注册等。

### 源码版本 4.2.0
Q: 为什么升级了源码版本？  
A: 4.2.0相对于2.13.0修复了一些bug，其实最主要的是我开始使用zookeeper3.5.6了 (233)

### 关于错误
由于个人对代码的理解可能出现错误，所有如果你觉得我注释的有问题，欢迎提出。

# Apache Curator

[![Build Status](https://travis-ci.org/apache/curator.svg?branch=master)](https://travis-ci.org/apache/curator)
[![Maven Central](https://img.shields.io/maven-central/v/org.apache.curator/apache-curator.svg)](http://search.maven.org/#search%7Cga%7C1%7Capache-curator)


## What's is Apache Curator?

Apache Curator is a Java/JVM client library for Apache ZooKeeper[1], a distributed coordination service.

Apache Curator includes a high-level API framework and utilities to make using Apache ZooKeeper much easier and more reliable. It also includes recipes for common use cases and extensions such as service discovery and a Java 8 asynchronous DSL.
For more details, please visit the project website: http://curator.apache.org/

[1] Apache ZooKeeper https://zookeeper.apache.org/

