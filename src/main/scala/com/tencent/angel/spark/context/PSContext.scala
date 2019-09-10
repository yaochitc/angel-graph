package com.tencent.angel.spark.context

import com.tencent.angel.AngelDeployMode
import com.tencent.angel.ml.matrix.{MatrixContext, MatrixMeta}
import org.apache.spark._

import scala.collection.Map
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}
import com.tencent.angel.spark.models.PSVector


abstract class PSContext {
  private[spark] def conf: Map[String, String]

  protected def stop()

  def createMatrix(matrixContext : MatrixContext): MatrixMeta

  def createMatrix(rows: Int, cols: Long, validIndexNum: Long, rowInBlock: Int, colInBlock: Long,
                   rowType: RowType, additionalConfiguration:Map[String, String] = Map()): MatrixMeta

  def createDenseMatrix(rows: Int, cols: Long, rowInBlock: Int, colInBlock: Long,
                        rowType: RowType = RowType.T_DOUBLE_DENSE,
                        additionalConfiguration:Map[String, String] = Map()): MatrixMeta

  def createSparseMatrix(rows: Int, cols: Long, range: Long, rowInBlock: Int, colInBlock: Long,
                         rowType: RowType = RowType.T_DOUBLE_SPARSE,
                         additionalConfiguration:Map[String, String] = Map()): MatrixMeta

  def destroyMatrix(matrixId: Int)

  def createVector(dim: Long, t: RowType, poolCapacity: Int, range: Long,
                   additionalConfiguration:Map[String, String] = Map()): PSVector

  def duplicateVector(originVector: PSVector): PSVector

  def destroyVector(vector: PSVector)

  def destroyVectorPool(vector: PSVector): Unit

  def refreshMatrix(): Unit

  def getMatrixMeta(matrixId: Int): Option[MatrixMeta]

  def save(ctx: ModelSaveContext)

  def checkpoint(checkpointId: Int, ctx: ModelSaveContext)

  def load(ctx: ModelLoadContext)

  def recover(checkpointId: Int, ctx: ModelLoadContext)
}

object PSContext {
  private var _instance: PSContext = _
  private var failCause: Exception = _

  def getOrCreate(sc: SparkContext): PSContext = {
    _instance = instance()

    if (_instance == null) {
      throw new Exception(s"init PSContext failed, $failCause")
    }

    _instance.conf.foreach {
      case (key, value) => sc.setLocalProperty(key, value)
    }
    _instance
  }

  def instance(): PSContext = {
    if (_instance == null) {
      classOf[PSContext].synchronized {
        if (_instance == null) {
          try {
            val env = SparkEnv.get
            _instance = AngelPSContext(env.executorId, env.conf)
          } catch {
            case e: Exception =>
              _instance = null
              e.printStackTrace()
              throw new AngelException("init AngelPSContext fail, please check logs of master of angel")
          }
        }
      }
    }
    _instance
  }

  /**
    * Clean up PSContext.
    */
  def stop(): Unit = {
    PSContext._instance.stop()
    PSContext._instance = null

    AngelPSContext.stopAngel()
  }

  private def isLocalMode(conf: SparkConf): Boolean = {
    val master = conf.get("spark.master", "")
    master.toLowerCase.startsWith("local")
  }

  private def isAngelMode(conf: SparkConf): Boolean = {
    if (isLocalMode(conf))
      return false

    val psMode = conf.getOption("spark.ps.mode")
    if (psMode.isDefined && psMode.get == AngelDeployMode.YARN.toString) {
      true
    } else {
      false
    }
  }

  private[spark] def getTaskId: Int = {
    val tc = TaskContext.get()
    if (tc == null) -1 else tc.partitionId()
  }
}