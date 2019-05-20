service SentimentAnalysisServer {
			bool ping(),
			bool process(1:list<string>fileNames)
		}
