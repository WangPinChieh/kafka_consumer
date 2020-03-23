using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;

namespace kafka_consumer
{
	class Program
	{
		static void Main(string[] args)
		{
			var consumerConfig = new ConsumerConfig
			{
				GroupId = "test-consumer-group",
				BootstrapServers = "localhost:9092",
				// Note: The AutoOffsetReset property determines the start offset in the event
				// there are not yet any committed offsets for the consumer group for the
				// topic/partitions of interest. By default, offsets are committed
				// automatically, so in this example, consumption will only start from the
				// earliest message in the topic 'my-topic' the first time you run the program.
				AutoOffsetReset = AutoOffsetReset.Earliest
			};
			using (var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build())
			{
				consumer.Subscribe("MyVeryFirstTopic");
				var cancellationTokenSource = new CancellationTokenSource();
				Console.CancelKeyPress += (sender, e) =>
				{
					e.Cancel = true;
					cancellationTokenSource.Cancel();
				};
				try
				{
					while (true)
					{
						try
						{
							var consumeResult = consumer.Consume(cancellationTokenSource.Token);
							Console.WriteLine($"Consumed message {consumeResult.Value} at {consumeResult.TopicPartitionOffset}");
						}
						catch (ConsumeException e)
						{
							Console.WriteLine($"Error occured: {e.Error.Reason}");
							throw;
						}
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e);
					throw;
				}
			}
		}
	}
}