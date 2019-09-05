package com.tencent.angel.graph.client.getedgefeature;

import com.tencent.angel.graph.client.getnodefeature.PartGetNodeLongFeatureResult;
import com.tencent.angel.graph.data.feature.LongFeatures;
import com.tencent.angel.graph.data.graph.Edge;
import com.tencent.angel.graph.data.graph.EdgeId;
import com.tencent.angel.graph.ps.storage.vector.GraphServerRow;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetEdgeLongFeature extends GetFunc {
	/**
	 * Create a new GetEdgeLongFeature.
	 *
	 * @param param parameter
	 */
	public GetEdgeLongFeature(GetEdgeFeatureParam param) {
		super(param);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartGetEdgeFeatureParam param = (PartGetEdgeFeatureParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
		GraphServerRow row = (GraphServerRow) part.getRow(0);

		// Results
		LongFeatures[] results = new LongFeatures[param.getEdgeIds().length];

		// Get feature for each node
		EdgeId[] edgeIds = param.getEdgeIds();
		for (int i = 0; i < edgeIds.length; i++) {
			results[i] = getFeature(row.getEdge(edgeIds[i]), param.getFids());
		}

		return new PartGetEdgeLongFeatureResult(part.getPartitionKey().getPartitionId(), results);
	}

	private LongFeatures getFeature(Edge edge, int[] fids) {
		int[] featureNums = new int[fids.length];
		List<Long> featureValues = new ArrayList<>();

		int[] longFeatureIndices = edge.getLongFeatureIndices();
		long[] longFeatures = edge.getLongFeatures();

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
		int[] offsets = ((GetEdgeFeatureParam) param).getOffsets();
		EdgeId[] edgeIds = ((GetEdgeFeatureParam) param).getEdgeIds();
		int len = ((GetEdgeFeatureParam) param).getEdgeIds().length;

		Map<EdgeId, LongFeatures> features = new Object2ObjectOpenHashMap<>(len);
		for (PartitionGetResult result : partResults) {
			PartGetNodeLongFeatureResult getNodeFeatureResult = (PartGetNodeLongFeatureResult) result;
			LongFeatures[] nodeResults = getNodeFeatureResult.getNodeFeatures();
			int startIndex = getNodeFeatureResult.getPartId() == 0 ? 0 : offsets[getNodeFeatureResult.getPartId() - 1];
			int endIndex = offsets[getNodeFeatureResult.getPartId()];
			for (int i = startIndex; i < endIndex; i++) {
				features.put(edgeIds[i], nodeResults[i - startIndex]);
			}
		}

		return new GetEdgeLongFeatureResult(features);
	}
}
