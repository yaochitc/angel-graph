package com.tencent.angel.algorithm.model

import com.intel.analytics.bigdl.dataset.{MiniBatch, SparseMiniBatch}
import com.intel.analytics.bigdl.nn.Graph.ModuleNode
import com.intel.analytics.bigdl.nn.keras.{Input, KerasLayerWrapper}
import com.intel.analytics.bigdl.nn.{MM, Sigmoid}
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl.tensor.TensorNumericMath.TensorNumeric
import com.intel.analytics.bigdl.utils.Shape
import com.intel.analytics.zoo.pipeline.api.keras.layers.Merge
import com.intel.analytics.zoo.pipeline.api.keras.models.Model
import com.tencent.angel.algorithm.aggregator.AggregatorType.AggregatorType
import com.tencent.angel.algorithm.encoder.SageEncoder
import com.tencent.angel.graph.client.IGraph
import com.tencent.angel.graph.ops.NeighborOps._
import com.tencent.angel.graph.ops.SampleOps.sampleNode

import scala.reflect.ClassTag

class GraphSage[T: ClassTag](nodeType: Int,
                             edgeType: Int,
                             metapath: Array[Array[Int]],
                             fanouts: Array[Int],
                             dim: Int,
                             aggregatorType: AggregatorType,
                             concat: Boolean,
                             maxId: Int,
                             embeddingDim: Int,
                             denseFeatureDim: Int,
                             sparseFeatureMaxIds: Array[Int],
                             numNegs: Int = 5)
                            (implicit ev: TensorNumeric[T]) extends BaseModel[T] {
  private val countPerLayer = fanouts.scanLeft(1) { case (acc, item) => acc * item }
  private val numLayer = fanouts.length

  override def sample(input: Array[Long], graph: IGraph): MiniBatch[T] = {
    val batchSize = input.length
    val (pos, _, _) = sampleNeighbor(input, Array(edgeType), 1)(graph)
    val neg = sampleNode(nodeType, batchSize * numNegs)(graph)

    val srcTensors = samples2Tensors(input, graph, 1)
    val posTensors = samples2Tensors(pos.flatten, graph, 1)
    val negTensors = samples2Tensors(neg, graph, numNegs)

    val labelTensor = Tensor(batchSize, numNegs + 1).zero()

    for (b <- 0 until batchSize) {
      labelTensor.setValue(b, 1, ev.one)
    }

    new SparseMiniBatch[T](srcTensors ++ posTensors ++ negTensors, Array(labelTensor))
  }

  private def samples2Tensors(samples: Array[Long], graph: IGraph, numSamples: Int): Array[Tensor[T]] = {
    val (nodes, _, _) = sampleFanout(samples, metapath, fanouts)(graph)
    val sampleSize = samples.length
    val batchSize = sampleSize / numSamples
    val featureTensors = (0 until numLayer + 1).map(i => Tensor[T](batchSize, countPerLayer(i) * numSamples))

    for (i <- 0 until numLayer + 1; b <- 0 until batchSize; n <- 0 until numSamples; count <- 0 until countPerLayer(i)) {
      featureTensors(i).setValue(b, ev.fromType(nodes(i)((b * numSamples + n) * countPerLayer(i) + count)))
    }
    featureTensors.toArray
  }

  override def buildModel(): Model[T] = {
    val numSparseFeatures = sparseFeatureMaxIds.length
    val srcTensors = countPerLayer.map(count =>
      (Input[T](inputShape = Shape(count)), Input[T](Shape(denseFeatureDim)), (0 until numSparseFeatures).map(i => Input[T](Shape(sparseFeatureMaxIds(i)))))
    )
    val posTensors = countPerLayer.map(count =>
      (Input[T](inputShape = Shape(count)), Input[T](Shape(denseFeatureDim)), (0 until numSparseFeatures).map(i => Input[T](Shape(sparseFeatureMaxIds(i)))))
    )
    val negTensors = countPerLayer.map(count =>
      (Input[T](inputShape = Shape(count * numNegs)), Input[T](Shape(denseFeatureDim)), (0 until numSparseFeatures).map(i => Input[T](Shape(sparseFeatureMaxIds(i)))))
    )

    val contextEncoder = new SageEncoder[T](numLayer, dim, aggregatorType, concat, maxId, embeddingDim, denseFeatureDim, sparseFeatureMaxIds)
    val targetEncoder = new SageEncoder[T](numLayer, dim, aggregatorType, concat, maxId, embeddingDim, denseFeatureDim, sparseFeatureMaxIds)

    val srcEmbedding = targetEncoder.encode(srcTensors)
    val posEmbedding = contextEncoder.encode(posTensors, "pos", isReplica = false)
    val negEmbedding = contextEncoder.encode(negTensors, "neg", isReplica = true)

    val posLogit = new KerasLayerWrapper[T](MM[T]()).inputs(srcEmbedding, posEmbedding)
    val negLogit = new KerasLayerWrapper[T](MM[T]()).inputs(srcEmbedding, negEmbedding)

    val logit = Merge[T](mode = "concat").inputs(posLogit, negLogit)
    val output = new KerasLayerWrapper[T](Sigmoid[T]()).inputs(logit)

    val inputs = Array.ofDim[ModuleNode[T]]((numLayer + 1) * (2 + numSparseFeatures) * 3)
    for (i <- 0 until numLayer + 1) {
      inputs(3 * i) = srcTensors(i)._1
      inputs(3 * i + 1) = srcTensors(i)._2

      for (j <- srcTensors(i)._3.indices) {
        inputs(3 * i + 2 + j) = srcTensors(i)._3(j)
      }

      inputs((numLayer + 1) * 3 + 3 * i) = posTensors(i)._1
      inputs((numLayer + 1) * 3 + 3 * i + 1) = posTensors(i)._2
      for (j <- posTensors(i)._3.indices) {
        inputs((numLayer + 1) * 3 + 3 * i + 2 + j) = posTensors(i)._3(j)
      }

      inputs((numLayer + 1) * 3 * 2 + 3 * i) = negTensors(i)._1
      inputs((numLayer + 1) * 3 * 2 + 3 * i + 1) = negTensors(i)._2
      for (j <- negTensors(i)._3.indices) {
        inputs((numLayer + 1) * 3 * 2 + 3 * i + 2 + j) = negTensors(i)._3(j)
      }
    }

    Model(inputs, output)
  }
}
