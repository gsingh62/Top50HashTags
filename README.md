# Top50HashTags

A flink job that finds the top 50 hash tags in twitter posts over last 2 days only.
There could be billions of posts in the last 2 days and they're all coming in realtime - a 5 minute delay in results
is allowed.
Example input from tweet database ->
{"text": "I love #Flink and #Streaming", "timestamp": 1729968800000}
