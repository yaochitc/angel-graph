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

import com.tencent.angel.graph.data.NodeEdgesPair;
import com.tencent.angel.graph.data.NodeIDWeightPairs;
import com.tencent.angel.graph.data.NodeWalkPair;
import com.tencent.angel.graph.data.feature.BinaryFeatures;
import com.tencent.angel.graph.data.feature.FloatFeatures;
import com.tencent.angel.graph.data.feature.LongFeatures;
import com.tencent.angel.graph.data.graph.EdgeId;

import java.util.Map;

/**
 * Graph operator interface
 */
public interface IGraph {

	void buildGlobalSampler();

	NodeWalkPair biasedSampleNeighbor(long[] nodeIds, Map<Long, NodeIDWeightPairs> parentNodeNeighbors, int[] edgeTypes,
									  int count, float p, float q);

	void initNeighbor(NodeEdgesPair[] nodeEdgesPairs);

	long[] sampleNode(int nodeType, int count);

	EdgeId[] sampleEdge(int edgeType, int count);

	Map<Long, Integer> getNodeType(long[] nodeIds);

	Map<Long, FloatFeatures> getNodeFloatFeature(long[] nodeIds, int[] fids);

	Map<Long, LongFeatures> getNodeLongFeature(long[] nodeIds, int[] fids);

	Map<Long, BinaryFeatures> getNodeBinaryFeature(long[] nodeIds, int[] fids);

	Map<EdgeId, FloatFeatures> getEdgeFloatFeature(EdgeId[] edgeIds, int[] fids);

	Map<EdgeId, LongFeatures> getEdgeLongFeature(EdgeId[] edgeIds, int[] fids);

	Map<EdgeId, BinaryFeatures> getEdgeBinaryFeature(EdgeId[] edgeIds, int[] fids);

	/**
	 * Get full neighbors for given edge types
	 *
	 * @param nodeIds   node ids
	 * @param edgeTypes edge types
	 * @return node id to result map
	 */
	Map<Long, NodeIDWeightPairs> getFullNeighbor(long[] nodeIds, int[] edgeTypes);

	Map<Long, NodeIDWeightPairs> getSortedFullNeighbor(long[] nodeIds, int[] edgeTypes);

	Map<Long, NodeIDWeightPairs> getTopKNeighbor(long[] nodeIds, int[] edgeTypes, int k);

	Map<Long, NodeIDWeightPairs> sampleNeighbor(long[] nodeIds, int[] edgeTypes, int count);
}
