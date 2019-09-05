package com.tencent.angel.graph.client.getedgefeature;

import com.tencent.angel.graph.client.getnodefeature.PartGetNodeFloatFeatureResult;
import com.tencent.angel.graph.data.feature.FloatFeatures;
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

public class GetEdgeFloatFeature extends GetFunc {
	/**
	 * Create a new GetEdgeFloatFeature.
	 *
	 * @param param parameter
	 */
	public GetEdgeFloatFeature(GetEdgeFeatureParam param) {
		super(param);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartGetEdgeFeatureParam param = (PartGetEdgeFeatureParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
		GraphServerRow row = (GraphServerRow) part.getRow(0);

		// Results
		FloatFeatures[] results = new FloatFeatures[param.getEdgeIds().length];

		// Get feature for each node
		EdgeId[] edgeIds = param.getEdgeIds();
		for (int i = 0; i < edgeIds.length; i++) {
			results[i] = getFeature(row.getEdge(edgeIds[i]), param.getFids());
		}

		return new PartGetNodeFloatFeatureResult(part.getPartitionKey().getPartitionId(), results);
	}

	private FloatFeatures getFeature(Edge edge, int[] fids) {
		int[] featureSizes = new int[fids.length];
		List<Float> featureValues = new ArrayList<>();

		int[] floatFeatureIndices = edge.getFloatFeatureIndices();
		float[] floatFeatures = edge.getFloatFeatures();

		// Get feature through feature index
		for (int i = 0; i < fids.length; i++) {
			int fid = fids[i];
			if (fid >= 0 && fid < floatFeatureIndices.length) {
				int pre = fid == 0 ? 0 : floatFeatureIndices[fid - 1];
				int cur = floatFeatureIndices[fid];
				for (int fidx = pre; fidx < cur; fidx++) {
					featureValues.add(floatFeatures[fidx]);
				}
				featureSizes[i] = cur - pre;
			} else {
				featureSizes[i] = 0;
			}
		}
		return new FloatFeatures(featureSizes, featureValues.toArray(new Float[0]));
	}

	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		int[] offsets = ((GetEdgeFeatureParam) param).getOffsets();
		EdgeId[] edgeIds = ((GetEdgeFeatureParam) param).getEdgeIds();
		int len = ((GetEdgeFeatureParam) param).getEdgeIds().length;

		Map<EdgeId, FloatFeatures> features = new Object2ObjectOpenHashMap<>(len);
		for (PartitionGetResult result : partResults) {
			PartGetEdgeFloatFeatureResult getEdgeFeatureResult = (PartGetEdgeFloatFeatureResult) result;
			FloatFeatures[] edgeFeatures = getEdgeFeatureResult.getEdgeFeatures();
			int startIndex = getEdgeFeatureResult.getPartId() == 0 ? 0 : offsets[getEdgeFeatureResult.getPartId() - 1];
			int endIndex = offsets[getEdgeFeatureResult.getPartId()];
			for (int i = startIndex; i < endIndex; i++) {
				features.put(edgeIds[i], edgeFeatures[i - startIndex]);
			}
		}

		return new GetEdgeFloatFeatureResult(features);
	}
}
