package cn.tongdun.streamx.addon.discovery

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.log4j.LogManager
import org.apache.zookeeper.CreateMode

import scala.util.control._

/**
  *  \* Created with IntelliJ IDEA.
  *  \* User: hongyi.zhou
  *  \* Date: 2020-06-04
  *  \* Time: 21:16
  *  \* Description: 
  *  \*/
class SessionConnectionListener(private var path: String, private var data: String) extends ConnectionStateListener {
    @transient private lazy val logger = LogManager.getLogger(classOf[SessionConnectionListener])

    override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
        if (newState == ConnectionState.LOST) {
            logger.error("[SessionConnectionListener]zk session timeout...")
            val loop = new Breaks
            loop.breakable {
                while (true) {
                    try {
                        if (client.getZookeeperClient.blockUntilConnectedOrTimedOut) {
                            client.create.creatingParentsIfNeeded.withMode(CreateMode.EPHEMERAL).forPath(path, data.getBytes("UTF-8"))
                            logger.info("[SessionConnectionListener]reconnect zk success!")
                            loop.break
                        }
                    }
                    catch {
                        case e: InterruptedException =>
                            loop.break
                        case e: Exception =>

                    }
                    Thread.sleep(1000)
                }
            }
        }
    }
}
