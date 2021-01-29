package com.zhy.streamx.core.util

import java.util.concurrent._

import com.google.common.util.concurrent.ThreadFactoryBuilder

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-06-03
  *  \* Time: 11:05
  *  \* Description: 线程工具类
  *  \*/
object ThreadUtils {
    def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
        val threadFactory = namedThreadFactory(prefix)
        Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
    }

    def newDaemonSingleThreadScheduledExecutor(prefix: String): ScheduledExecutorService = {
        val threadFactory = namedThreadFactory(prefix)
        val executor = new ScheduledThreadPoolExecutor(1, threadFactory)
        executor.setRemoveOnCancelPolicy(true)
        executor
    }

    def newDaemonCachedThreadPool(prefix: String, maxThreadNumber: Int, keepAliveSeconds: Int): ThreadPoolExecutor = {
        val threadFactory = namedThreadFactory(prefix)
        val threadPool = new ThreadPoolExecutor(
            maxThreadNumber, // corePoolSize: the max number of threads to create before queuing the tasks
            maxThreadNumber, // maximumPoolSize: because we use LinkedBlockingDeque, this one is not used
            keepAliveSeconds,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue[Runnable],
            threadFactory)
        threadPool.allowCoreThreadTimeOut(true)
        threadPool
    }

    def namedThreadFactory(prefix: String): ThreadFactory =
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build
}
