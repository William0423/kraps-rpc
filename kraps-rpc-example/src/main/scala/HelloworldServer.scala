/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

/**
  * Usage:
  * {{{
  *   java -server -Xms4096m -Xmx4096m -cp kraps-rpc-example_2.11-1.0.1-SNAPSHOT-jar-with-dependencies.jar HelloworldServer 10.32.240.1
  * }}}
  */
object HelloworldServer {

  def main(args: Array[String]): Unit = {

    val host = args(0)
    //    val host = "localhost"
    /**
      * 1、RpcEnvServerConfig可以定义一些参数、server名称（仅仅是一个标识）、bind地址和端口。
      */
    val config = RpcEnvServerConfig(new RpcConf(), "hello-server", host, 52345)

    /**
      * 2、通过NettyRpcEnvFactory这个工厂方法，生成RpcEnv，RpcEnv是整个Spark RPC的核心所在，后文会详细展开，
      */
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

    val helloEndpoint: RpcEndpoint = new HelloEndpoint(rpcEnv)

    /**
      * 4、把刚刚开发好的Endpoint交给Spark RPC管理其生命周期，用于响应外部请求。
      */
    rpcEnv.setupEndpoint("hello-service", helloEndpoint)

    /**
      * 5、调用awaitTermination来阻塞服务端监听请求并且处理。
      */
    rpcEnv.awaitTermination()


  }
}

/**
  * 第一步，定义一个HelloEndpoint继承自RpcEndpoint表明可以并发的调用该服务，如果继承自ThreadSafeRpcEndpoint则表明该Endpoint不允许并发。
  * @param rpcEnv
  */
class HelloEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {

  override def onStart(): Unit = {
    println("start hello endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHi(msg) => {
      //println(s"receive $msg")
      context.reply(s"$msg")
    }
    case SayBye(msg) => {
      //println(s"receive $msg")
      context.reply(s"bye, $msg")
    }
  }

  override def onStop(): Unit = {
    println("stop hello endpoint")
  }


}


case class SayHi(msg: String)

case class SayBye(msg: String)
