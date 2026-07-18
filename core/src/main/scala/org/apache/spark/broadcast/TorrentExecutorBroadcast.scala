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

package org.apache.spark.broadcast

import java.io.ObjectOutputStream

import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockId, BroadcastBlockId, RDDBlockId, StorageLevel}
import org.apache.spark.util.{KeyLock, Utils}

/** A broadcast whose value is assembled from persisted RDD blocks on each executor. */
private[spark] class TorrentExecutorBroadcast[T: ClassTag, U: ClassTag](
    @transient rdd: RDD[T],
    transform: Iterator[T] => U,
    id: Long) extends Broadcast[U](id) with Logging {

  private val rddId = rdd.id
  private val numBlocks = rdd.getNumPartitions
  private val broadcastId = BroadcastBlockId(id)
  @transient private lazy val _value = readBroadcastBlock()

  override protected def getValue(): U = {
    require(TaskContext.get() != null,
      "Executor-side broadcast values cannot be materialized on the driver")
    _value
  }

  private def readBroadcastBlock(): U = TorrentExecutorBroadcast.lock.withLock(broadcastId) {
    val blockManager = SparkEnv.get.blockManager
    blockManager.getLocalValues(broadcastId).orElse(blockManager.get[U](broadcastId)) match {
      case Some(result) if result.data.hasNext =>
        val value = result.data.next().asInstanceOf[U]
        releaseLock(broadcastId)
        value
      case _ =>
        val blocks = new Array[Array[T]](numBlocks)
        Random.shuffle((0 until numBlocks).toList).foreach { partitionId =>
          val blockId = RDDBlockId(rddId, partitionId)
          val result = blockManager.getLocalValues(blockId).orElse(blockManager.get[T](blockId))
            .getOrElse(throw new SparkException(s"Failed to fetch $blockId"))
          blocks(partitionId) = result.data.asInstanceOf[Iterator[T]].toArray
        }
        val value = transform(blocks.iterator.flatten)
        if (!blockManager.putSingle(
            broadcastId, value, StorageLevel.MEMORY_AND_DISK, tellMaster = true)) {
          throw new SparkException(s"Failed to store $broadcastId")
        }
        value
    }
  }

  private def releaseLock(blockId: BlockId): Unit = Option(TaskContext.get()) match {
    case Some(context) => context.addTaskCompletionListener[Unit](_ =>
      SparkEnv.get.blockManager.releaseLock(blockId))
    case None => SparkEnv.get.blockManager.releaseLock(blockId)
  }

  override protected def doUnpersist(blocking: Boolean): Unit =
    TorrentBroadcast.unpersist(id, removeFromDriver = false, blocking)

  override protected def doDestroy(blocking: Boolean): Unit =
    TorrentBroadcast.unpersist(id, removeFromDriver = true, blocking)

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    assertValid()
    out.defaultWriteObject()
  }
}

private object TorrentExecutorBroadcast {
  private val lock = new KeyLock[BroadcastBlockId]
}
