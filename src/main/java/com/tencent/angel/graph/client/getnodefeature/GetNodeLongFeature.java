package com.tencent.angel.graph.client.getnodefeature;

import com.tencent.angel.graph.data.feature.LongFeatures;
import com.tencent.angel.graph.data.graph.Node;
import com.tencent.angel.graph.ps.storage.vector.GraphServerRow;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetNodeLongFeature extends GetFunc {
	/**
	 * Create a new GetNodeLongFeature.
	 *
	 * @param param parameter
	 */
	public GetNodeLongFeature(GetNodeFeatureParam param) {
		super(param);
	}

	/**
	 * Create a empty GetNodeLongFeature
	 */
	public GetNodeLongFeature() {
		this(null);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartGetNodeFeatureParam param = (PartGetNodeFeatureParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
		GraphServerRow row = (GraphServerRow) part.getRow(0);

		// Results
		LongFeatures[] results = new LongFeatures[param.getNodeIds().length];

		// Get feature for each node
		long[] nodeIds = param.getNodeIds();
		for (int i = 0; i < nodeIds.length; i++) {
			results[i] = getFeature(row.getNode(nodeIds[i]), param.getFids());
		}

		return new PartGetNodeLongFeatureResult(part.getPartitionKey().getPartitionId(), results);
	}

	private LongFeatures getFeature(Node node, int[] fids) {
		int[] featureNums = new int[fids.length];
		List<Long> featureValues = new ArrayList<>();

		int[] longFeatureIndices = node.getLongFeatureIndices();
		long[] longFeatures = node.getLongFeatures();

		// Get feature through feature index
		for (int i = 0; i < fids.length; i++) {
			int fid = fids[i];
			if (fid >= 0 && fid < longFeatureIndices.length) {
				int pre = fid == 0 ? 0 : longFeatureIndices[fid - 1];
				int cur = longFeatureIndices[fid];
				for (int fidx = pre; fidx < cur; fidx++) {
					featureValues.add(longFeatures[fidx]);
				}
				featureNums[i] = cur - pre;
			} else {
				featureNums[i] = 0;
			}
		}
		return new LongFeatures(featureNums, featureValues.toArray(new Long[0]));
	}

	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		int[] offsets = ((GetNodeFeatureParam) param).getOffsets();
		long[] nodeIds = ((GetNodeFeatureParam) param).getNodeIds();
		int len = ((GetNodeFeatureParam) param).getNodeIds().length;

		Map<Long, LongFeatures> features = new Long2ObjectOpenHashMap<>(len);
		for (PartitionGetResult result : partResults) {
			PartGetNodeLongFeatureResult getNodeFeatureResult = (PartGetNodeLongFeatureResult) result;
			LongFeatures[] nodeResults = getNodeFeatureResult.getNodeFeatures();
			int startIndex = getNodeFeatureResult.getPartId() == 0 ? 0 : offsets[getNodeFeatureResult.getPartId() - 1];
			int endIndex = offsets[getNodeFeatureResult.getPartId()];
			for (int i = startIndex; i < endIndex; i++) {
				features.put(nodeIds[i], nodeResults[i - startIndex]);
			}
		}

		return new GetNodeLongFeatureResult(features);
	}
}
