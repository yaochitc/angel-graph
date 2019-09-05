package com.tencent.angel.graph.client.getedgefeature;

import com.tencent.angel.graph.data.feature.BinaryFeatures;
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

public class GetEdgeBinaryFeature extends GetFunc {
	/**
	 * Create a new GetEdgeBinaryFeature.
	 *
	 * @param param parameter
	 */
	public GetEdgeBinaryFeature(GetEdgeFeatureParam param) {
		super(param);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartGetEdgeFeatureParam param = (PartGetEdgeFeatureParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
		GraphServerRow row = (GraphServerRow) part.getRow(0);

		// Results
		BinaryFeatures[] results = new BinaryFeatures[param.getEdgeIds().length];

		// Get feature for each node
		EdgeId[] edgeIds = param.getEdgeIds();
		for (int i = 0; i < edgeIds.length; i++) {
			results[i] = getFeature(row.getEdge(edgeIds[i]), param.getFids());
		}

		return new PartGetEdgeBinaryFeatureResult(part.getPartitionKey().getPartitionId(), results);
	}

	private BinaryFeatures getFeature(Edge edge, int[] fids) {
		int[] featureSizes = new int[fids.length];
		List<Byte> featureValues = new ArrayList<>();

		int[] binaryFeatureIndices = edge.getBinaryFeatureIndices();
		byte[] binaryFeatures = edge.getBinaryFeatures();

		// Get feature through feature index
		for (int i = 0; i < fids.length; i++) {
			int fid = fids[i];
			if (fid >= 0 && fid < binaryFeatureIndices.length) {
				int pre = fid == 0 ? 0 : binaryFeatureIndices[fid - 1];
				int cur = binaryFeatureIndices[fid];
				for (int fidx = pre; fidx < cur; fidx++) {
					featureValues.add(binaryFeatures[fidx]);
				}
				featureSizes[i] = cur - pre;
			} else {
				featureSizes[i] = 0;
			}
		}
		return new BinaryFeatures(featureSizes, featureValues.toArray(new Byte[0]));
	}

	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		int[] offsets = ((GetEdgeFeatureParam) param).getOffsets();
		EdgeId[] edgeIds = ((GetEdgeFeatureParam) param).getEdgeIds();
		int len = ((GetEdgeFeatureParam) param).getEdgeIds().length;

		Map<EdgeId, BinaryFeatures> features = new Object2ObjectOpenHashMap<>(len);
		for (PartitionGetResult result : partResults) {
			PartGetEdgeBinaryFeatureResult getNodeFeatureResult = (PartGetEdgeBinaryFeatureResult) result;
			BinaryFeatures[] edgeFeatures = getNodeFeatureResult.getEdgeFeatures();
			int startIndex = getNodeFeatureResult.getPartId() == 0 ? 0 : offsets[getNodeFeatureResult.getPartId() - 1];
			int endIndex = offsets[getNodeFeatureResult.getPartId()];
			for (int i = startIndex; i < endIndex; i++) {
				features.put(edgeIds[i], edgeFeatures[i - startIndex]);
			}
		}

		return new GetEdgeBinaryFeatureResult(features);
	}
}
