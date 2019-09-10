package com.tencent.angel.spark.context

import com.tencent.angel.ml.math2.utils.RowType
import org.apache.spark.SparkException
import sun.misc.Cleaner
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.models.impl.PSVectorImpl

/**
  * PSVectorPool delegate a memory space on PS servers,
  * which hold `capacity` number vectors with `numDimensions` dimension.
  * The dimension of PSVectors in one PSVectorPool is the same.
  *
  * A PSVectorPool is like a Angel Matrix.
  *
  * @param id        PSVectorPool unique id
  * @param dimension Dimension of vectors
  * @param capacity  Capacity of pool
  */

private[spark] class PSVectorPool(
                                   val id: Int,
                                   val dimension: Long,
                                   val capacity: Int,
                                   val rowType: RowType) {

  val cleaners = new java.util.WeakHashMap[PSVector, Cleaner]
  val bitSet = new java.util.BitSet(capacity)
  var destroyed = false
  var size = 0

  private[spark] def allocate(): PSVector = {
    if (destroyed) {
      throw new SparkException("This vector pool has been destroyed!")
    }

    if (size > math.max(capacity * 0.9, 4)) {
      System.gc()
    }

    tryOnce match {
      case Some(toReturn) => return toReturn
      case None =>
    }

    System.gc()
    Thread.sleep(100L)

    // Try again
    tryOnce match {
      case Some(toReturn) => toReturn
      case None => throw new SparkException("This vector pool is full!")
    }

  }

  private def tryOnce: Option[PSVector] = {
    bitSet.synchronized {
      if (size < capacity) {
        val index = bitSet.nextClearBit(0)
        bitSet.set(index)
        size += 1
        return Some(doCreateOne(index))
      }
    }
    None
  }

  private def doCreateOne(index: Int): PSVector = {
    val vector = new PSVectorImpl(id, index, dimension, rowType)
    val task = new CleanTask(id, index)
    cleaners.put(vector, Cleaner.create(vector, task))
    vector
  }

  private class CleanTask(poolId: Int, index: Int) extends Runnable {
    def run(): Unit = {
      bitSet.synchronized {
        bitSet.clear(index)
        size -= 1
      }
    }
  }

  private[spark] def delete(key: PSVector): Unit = {
    cleaners.remove(key).clean()
  }

  private[spark] def destroy(): Unit = {
    destroyed = true
  }
}

object PSVectorPool {
  val DEFAULT_POOL_CAPACITY = 10
}