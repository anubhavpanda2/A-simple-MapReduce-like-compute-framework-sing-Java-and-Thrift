service SentimentAnalysiscomputeNode {
			bool ping(),
			bool ComputeMap(1:string fileName,2:double probability),
			bool Computesort()
		}
//		thrift --gen java SentimentAnalysiscomputeNode.thrift
