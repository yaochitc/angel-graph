/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.graph.client;

import com.tencent.angel.graph.client.buildsampler.BuildSampler;
import com.tencent.angel.graph.client.buildsampler.BuildSamplerParam;
import com.tencent.angel.graph.client.buildsampler.BuildSamplerResult;
import com.tencent.angel.graph.client.getedgefeature.*;
import com.tencent.angel.graph.client.getfullneighbor.GetFullNeighbor;
import com.tencent.angel.graph.client.getfullneighbor.GetFullNeighborParam;
import com.tencent.angel.graph.client.getfullneighbor.GetFullNeighborResult;
import com.tencent.angel.graph.client.getnodefeature.*;
import com.tencent.angel.graph.client.getnodetype.GetNodeType;
import com.tencent.angel.graph.client.getnodetype.GetNodeTypeParam;
import com.tencent.angel.graph.client.getnodetype.GetNodeTypeResult;
import com.tencent.angel.graph.client.getsortedfullneighbor.GetSortedFullNeighbor;
import com.tencent.angel.graph.client.getsortedfullneighbor.GetSortedFullNeighborParam;
import com.tencent.angel.graph.client.getsortedfullneighbor.GetSortedFullNeighborResult;
import com.tencent.angel.graph.client.gettopkneighbor.GetTopkNeighbor;
import com.tencent.angel.graph.client.gettopkneighbor.GetTopkNeighborParam;
import com.tencent.angel.graph.client.gettopkneighbor.GetTopkNeighborResult;
import com.tencent.angel.graph.client.sampleedge.SampleEdge;
import com.tencent.angel.graph.client.sampleedge.SampleEdgeParam;
import com.tencent.angel.graph.client.sampleedge.SampleEdgeResult;
import com.tencent.angel.graph.client.sampleneighbor.SampleNeighbor;
import com.tencent.angel.graph.client.sampleneighbor.SampleNeighborParam;
import com.tencent.angel.graph.client.sampleneighbor.SampleNeighborResult;
import com.tencent.angel.graph.client.samplenode.SampleNode;
import com.tencent.angel.graph.client.samplenode.SampleNodeParam;
import com.tencent.angel.graph.client.samplenode.SampleNodeResult;
import com.tencent.angel.graph.data.NodeWalkPair;
import com.tencent.angel.graph.data.graph.EdgeId;
import com.tencent.angel.graph.data.NodeIDWeightPairs;
import com.tencent.angel.graph.data.feature.BinaryFeatures;
import com.tencent.angel.graph.data.feature.FloatFeatures;
import com.tencent.angel.graph.data.feature.LongFeatures;
import com.tencent.angel.psagent.matrix.MatrixClient;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.*;

/**
 * PS Graph
 */
public class PSGraph implements IGraph {

	/**
	 * PS matrix client
	 */
	private final MatrixClient matrixClient;

	private Map<Integer, Sampler<Integer>> nodeSamplers;
	private Map<Integer, Sampler<Integer>> edgeSamplers;

	public PSGraph(MatrixClient matrixClient) {
		this.matrixClient = matrixClient;
	}

	public void buildGlobalSampler() {
		BuildSamplerResult buildSamplerResult = ((BuildSamplerResult) matrixClient.get(new BuildSampler(
						new BuildSamplerParam(matrixClient.getMatrixId()))));

		nodeSamplers = new Int2ObjectOpenHashMap<>();
		Map<Integer, Map<Integer, Float>> nodeWeightSums = buildSamplerResult.getNodeWeightSum();
		for (Map.Entry<Integer, Map<Integer, Float>> entry : nodeWeightSums.entrySet()) {
			Map<Integer, Float> part2Weights = entry.getValue();
			Integer[] parts = new Integer[part2Weights.size()];
			float[] weights = new float[part2Weights.size()];
			int i = 0;
			for (Map.Entry<Integer, Float> part2Weight : part2Weights.entrySet()) {
				parts[i] = part2Weight.getKey();
				weights[i] = part2Weight.getValue();
				i++;
			}

			Sampler<Integer> nodeSampler = new CompactSampler<>();
			nodeSampler.init(parts, weights);

			int nodeType = entry.getKey();
			nodeSamplers.put(nodeType, nodeSampler);
		}

		edgeSamplers = new Int2ObjectOpenHashMap<>();
		Map<Integer, Map<Integer, Float>> edgeWeightSum = buildSamplerResult.getEdgeWeightSum();
		for (Map.Entry<Integer, Map<Integer, Float>> entry : edgeWeightSum.entrySet()) {
			Map<Integer, Float> part2Weights = entry.getValue();
			Integer[] parts = new Integer[part2Weights.size()];
			float[] weights = new float[part2Weights.size()];
			int i = 0;
			for (Map.Entry<Integer, Float> part2Weight : part2Weights.entrySet()) {
				parts[i] = part2Weight.getKey();
				weights[i] = part2Weight.getValue();
				i++;
			}

			Sampler<Integer> edgeSampler = new CompactSampler<>();
			edgeSampler.init(parts, weights);

			int edgeType = entry.getKey();
			edgeSamplers.put(edgeType, edgeSampler);
		}
	}

	@Override
	public NodeWalkPair biasedSampleNeighbor(long[] nodeIds, Map<Long, NodeIDWeightPairs> parentNodeNeighbors,
																					 int[] edgeTypes, int count, float p, float q) {
		Map<Long, NodeIDWeightPairs> id2Neighbors = getSortedFullNeighbor(nodeIds, edgeTypes);
		Map<Long, long[]> curWalkNeighbors = new Long2ObjectOpenHashMap<>();
		for (int i = 0; i < nodeIds.length; i++) {
			long nodeId = nodeIds[i];
			NodeIDWeightPairs childNeighbors = id2Neighbors.get(nodeId);
			if (childNeighbors.size() == 0) {
				curWalkNeighbors.put(nodeId, new long[0]);
				continue;
			}

			int j = 0;
			int k = 0;
			NodeIDWeightPairs parentNeighbors = parentNodeNeighbors.get(nodeId);
			float[] weights = new float[childNeighbors.size()];
			Long[] childNodeIds = new Long[childNeighbors.size()];
			while (j < childNeighbors.size() && k < parentNeighbors.size()) {
				long childNodeId = childNeighbors.getNodeIds()[j];
				float childNodeWeight = childNeighbors.getNodeWeights()[j];
				long parentNodeId = parentNeighbors.getNodeIds()[k];
				if (childNodeId < parentNodeId) {
					if (childNodeId != nodeId) {
						weights[j] = childNodeWeight / q;
					} else {
						weights[j] = childNodeWeight / p;
					}
					childNodeIds[j] = childNodeId;
					j++;
				} else if (childNodeId == parentNodeId) {
					weights[j] = childNodeWeight;
					childNodeIds[j] = childNodeId;
					j++;
					k++;
				} else {
					k++;
				}
			}

			while (j < childNeighbors.size()) {
				long childNodeId = childNeighbors.getNodeIds()[j];
				float childNodeWeight = childNeighbors.getNodeWeights()[j];
				if (childNodeId != nodeId) {
					weights[j] = childNodeWeight / q;
				} else {
					weights[j] = childNodeWeight / p;
				}
				childNodeIds[j] = childNodeId;
				j++;
			}

			Sampler<Long> sampler = new CompactSampler<>();
			sampler.init(childNodeIds, weights);

			long[] sampledNeighbors = new long[count];
			for (int l = 0; l < count; l++) {
				sampledNeighbors[l] = sampler.sample();
			}

			curWalkNeighbors.put(nodeId, sampledNeighbors);
		}

		return new NodeWalkPair(curWalkNeighbors, id2Neighbors);
	}


	@Override
	public long[] sampleNode(int nodeType, int count) {
		Map<Integer, Integer> countPerPart = new Int2IntOpenHashMap();
		Sampler<Integer> sampler = nodeSamplers.get(nodeType);
		for (int i = 0; i < count; i++) {
			int part = sampler.sample();
			countPerPart.put(part, countPerPart.computeIfAbsent(part, k -> 0) + 1);
		}

		return ((SampleNodeResult) matrixClient.get(new SampleNode(
						new SampleNodeParam(matrixClient.getMatrixId(), nodeType, count, countPerPart)
		))).getNodeIds();
	}

	@Override
	public EdgeId[] sampleEdge(int edgeType, int count) {
		Map<Integer, Integer> countPerPart = new Int2IntOpenHashMap();
		Sampler<Integer> sampler = edgeSamplers.get(edgeType);
		for (int i = 0; i < count; i++) {
			int part = sampler.sample();
			countPerPart.put(part, countPerPart.computeIfAbsent(part, k -> 0) + 1);
		}

		return ((SampleEdgeResult) matrixClient.get(new SampleEdge(
						new SampleEdgeParam(matrixClient.getMatrixId(), edgeType, count, countPerPart)
		))).getEdgeIds();
	}

	@Override
	public Map<Long, Integer> getNodeType(long[] nodeIds) {
		return ((GetNodeTypeResult) matrixClient.get(new GetNodeType(
						new GetNodeTypeParam(matrixClient.getMatrixId(), nodeIds)
		))).getNodeIdToNodeTypes();
	}

	@Override
	public Map<Long, FloatFeatures> getNodeFloatFeature(long[] nodeIds, int[] fids) {
		return ((GetNodeFloatFeatureResult) matrixClient.get(new GetNodeFloatFeature(
						new GetNodeFeatureParam(matrixClient.getMatrixId(), nodeIds, fids))))
						.getNodeFeatures();
	}

	@Override
	public Map<Long, LongFeatures> getNodeLongFeature(long[] nodeIds, int[] fids) {
		return ((GetNodeLongFeatureResult) matrixClient.get(new GetNodeLongFeature(
						new GetNodeFeatureParam(matrixClient.getMatrixId(), nodeIds, fids))))
						.getNodeFeatures();
	}

	@Override
	public Map<Long, BinaryFeatures> getNodeBinaryFeature(long[] nodeIds, int[] fids) {
		return ((GetNodeBinaryFeatureResult) matrixClient.get(new GetNodeBinaryFeature(
						new GetNodeFeatureParam(matrixClient.getMatrixId(), nodeIds, fids))))
						.getNodeFeatures();
	}

	@Override
	public Map<EdgeId, FloatFeatures> getEdgeFloatFeature(EdgeId[] edgeIds, int[] fids) {
		return ((GetEdgeFloatFeatureResult) matrixClient.get(new GetEdgeLongFeature(
						new GetEdgeFeatureParam(matrixClient.getMatrixId(), edgeIds, fids))))
						.getEdgeFeatures();
	}

	@Override
	public Map<EdgeId, LongFeatures> getEdgeLongFeature(EdgeId[] edgeIds, int[] fids) {
		return ((GetEdgeLongFeatureResult) matrixClient.get(new GetEdgeLongFeature(
						new GetEdgeFeatureParam(matrixClient.getMatrixId(), edgeIds, fids))))
						.getEdgeFeatures();
	}

	@Override
	public Map<EdgeId, BinaryFeatures> getEdgeBinaryFeature(EdgeId[] edgeIds, int[] fids) {
		return ((GetEdgeBinaryFeatureResult) matrixClient.get(new GetEdgeBinaryFeature(
						new GetEdgeFeatureParam(matrixClient.getMatrixId(), edgeIds, fids))))
						.getEdgeFeatures();
	}

	@Override
	public Map<Long, NodeIDWeightPairs> getFullNeighbor(long[] nodeIds, int[] edgeTypes) {
		return ((GetFullNeighborResult) matrixClient.get(new GetFullNeighbor(
						new GetFullNeighborParam(matrixClient.getMatrixId(), nodeIds, edgeTypes))))
						.getNodeIdToNeighbors();
	}

	@Override
	public Map<Long, NodeIDWeightPairs> getSortedFullNeighbor(long[] nodeIds, int[] edgeTypes) {
		return ((GetSortedFullNeighborResult) matrixClient.get(new GetSortedFullNeighbor(
						new GetSortedFullNeighborParam(matrixClient.getMatrixId(), nodeIds, edgeTypes))))
						.getNodeIdToNeighbors();
	}

	@Override
	public Map<Long, NodeIDWeightPairs> getTopKNeighbor(long[] nodeIds, int[] edgeTypes, int k) {
		return ((GetTopkNeighborResult) matrixClient.get(new GetTopkNeighbor(
						new GetTopkNeighborParam(matrixClient.getMatrixId(), nodeIds, edgeTypes, k))))
						.getNodeIdToNeighbors();
	}

	@Override
	public Map<Long, NodeIDWeightPairs> sampleNeighbor(long[] nodeIds, int[] edgeTypes, int count) {
		return ((SampleNeighborResult) matrixClient.get(new SampleNeighbor(
						new SampleNeighborParam(matrixClient.getMatrixId(), nodeIds, edgeTypes, count))))
						.getNodeIdToNeighbors();
	}
}
