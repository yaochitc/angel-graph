package com.tencent.angel.spark.models

import java.util.concurrent.Future

import com.tencent.angel.ml.math2.utils.RowType

import scala.collection.Map
import org.apache.spark.SparkException
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.update.base.{UpdateFunc, VoidResult}
import com.tencent.angel.spark.context.PSContext

/**
  * PSVector is a vector store on the PS nodes
  */

abstract class PSVector extends PSModel {
  val poolId: Int
  val id: Int
  val dimension: Long
  val rowType: RowType
  @transient private[models] var deleted = false

  def pull(): Vector

  /*
  note: calling this api would modify the indices array passed in
   */
  def pull(indices: Array[Long]): Vector

  /*
  note: calling this api would modify the indices array passed in
   */
  def pull(indices: Array[Int]): Vector

  def increment(delta: Vector): this.type

  def update(local: Vector): this.type

  def push(local: Vector): this.type

  def reset: this.type

  def fill(value: Double): this.type

  def psfGet(func: GetFunc): GetResult

  def psfUpdate(func: UpdateFunc): Future[VoidResult]

  def delete(): Unit



  def asyncPull(): Future[Vector]

  /*
  note: calling this api would modify the indices array passed in
   */
  def asyncPull(indices: Array[Long]): Future[Vector]

  /*
  note: calling this api would modify the indices array passed in
   */
  def asyncPull(indices: Array[Int]): Future[Vector]

  def asyncIncrement(delta: Vector): Future[VoidResult]

  def asyncUpdate(local: Vector): Future[VoidResult]

  def asyncPush(local: Vector): Future[VoidResult]

  def asyncReset: Future[VoidResult]

  def asyncFill(value: Double): Future[VoidResult]

  def asyncPsfGet(func: GetFunc): Future[GetResult]

  def assertValid(): this.type = {
    if (deleted)
      throw new SparkException("This vector has been deleted!")
    this
  }
}

object PSVector{
  def duplicate[K <: PSVector](original: K): K = {
    PSContext.instance().duplicateVector(original).asInstanceOf[K]
  }

  def dense(dimension: Long, capacity: Int = 20, rowType: RowType = RowType.T_DOUBLE_DENSE,
            additionalConfiguration:Map[String, String] = Map()): PSVector = {
    PSContext.instance().createVector(dimension, rowType, capacity, dimension, additionalConfiguration)
  }

  /**
    * @param maxRange if maxRange > 0, colId range is [0, maxRange),
    *                 if maxRange = -1, colId range is (Long.MinValue, Long.MaxValue)
    */
  def longKeySparse(dim: Long,
                    maxRange: Long,
                    capacity: Int = 20,
                    rowType: RowType = RowType.T_DOUBLE_SPARSE_LONGKEY,
                    additionalConfiguration:Map[String, String] = Map()): PSVector = {
    sparse(dim, capacity, maxRange, rowType, additionalConfiguration)
  }

  def sparse(dimension: Long, capacity: Int, range: Long, rowType: RowType,
             additionalConfiguration:Map[String, String]): PSVector = {
    PSContext.instance().createVector(dimension, rowType, capacity, range, additionalConfiguration)
  }

  def sparse(dimension: Long, capacity: Int = 20, rowType: RowType = RowType.T_DOUBLE_SPARSE_LONGKEY,
             additionalConfiguration:Map[String, String] = Map()): PSVector = {
    sparse(dimension, capacity, dimension, rowType, additionalConfiguration)
  }
}
