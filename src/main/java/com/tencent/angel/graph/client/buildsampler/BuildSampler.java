package com.tencent.angel.graph.client.buildsampler;

import com.tencent.angel.graph.ps.storage.vector.GraphServerRow;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.List;
import java.util.Map;

public class BuildSampler extends GetFunc {
	/**
	 * Create BuildSampler
	 *
	 * @param param parameter
	 */
	public BuildSampler(BuildSamplerParam param) {
		super(param);
	}

	/**
	 * Create a empty BuildSampler
	 */
	public BuildSampler() {
		super(null);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
		GraphServerRow row = (GraphServerRow) part.getRow(0);
		row.buildNodeSampler();
		row.buildEdgeSampler();
		return new PartBuildSamplerResult(part.getPartitionKey().getPartitionId(),
						row.getNodeWeightSums(),
						row.getEdgeWeightSums());
	}

	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		Map<Integer, Map<Integer, Float>> nodeWeightSum = new Int2ObjectOpenHashMap<>();
		Map<Integer, Map<Integer, Float>> edgeWeightSum = new Int2ObjectOpenHashMap<>();
		for (PartitionGetResult result : partResults) {
			PartBuildSamplerResult getWeightSumResult = (PartBuildSamplerResult) result;
			int partId = getWeightSumResult.getPartId();

			Map<Integer, Float> nodeWeightSums = getWeightSumResult.getNodeWeightSums();
			for (Map.Entry<Integer, Float> entry : nodeWeightSums.entrySet()) {
				int nodeType = entry.getKey();
				float weight = entry.getValue();
				Map<Integer, Float> part2Weights = nodeWeightSum.computeIfAbsent(nodeType, k -> new Int2ObjectOpenHashMap<>());
				part2Weights.put(partId, weight);
			}

			Map<Integer, Float> edgeWeightSums = getWeightSumResult.getEdgeWeightSums();
			for (Map.Entry<Integer, Float> entry : edgeWeightSums.entrySet()) {
				int edgeType = entry.getKey();
				float weight = entry.getValue();
				Map<Integer, Float> part2Weights = edgeWeightSum.computeIfAbsent(edgeType, k -> new Int2ObjectOpenHashMap<>());
				part2Weights.put(partId, weight);
			}
		}

		return new BuildSamplerResult(nodeWeightSum, edgeWeightSum);
	}
}