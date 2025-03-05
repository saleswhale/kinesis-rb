# Kinesis (Ruby)

An attempt (bastardized one at that) to port https://github.com/NerdWalletOSS/kinesis-python to Ruby, using threads instead of processes

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'kinesis', git: 'https://github.com/saleswhale/kinesis-rb'
```

And then execute:

    $ bundle install

## Overview

All of the functionality is wrapped into `Kinesis::Consumer` and `Kinesis::Producer`

#### Consumer

The consumer works by launching a thread per shard in the stream, then implementing the Ruby #each protocol

```ruby
require 'kinesis/consumer'

consumer = Kinesis::Consumer.new(stream_name: 'your-stream-here')

consumer.each do |message| # can also be "for message in consumer" if you prefer it
  # suppose I am expecting a JSONified payload
  payload = JSON.parse(message.data)

  puts "Got payload: #{payload}"
end
```

Messages received from each of the shard threads are passed back to the main thread through Ruby's thread-safe
Queue, yielded for processing. Messages are not strictly ordered (property of Kinesis).

##### Consumer: Locking and Checkpointing

When deploying an application with multiple instances, DynamoDB can be leveraged to coordinate which instance
is responsible for which shard.

A "state" is maintained in DynamoDB , detailing which node (consumerId) is responsible for which shard, and which
"checkpoint" it last synced from, to start syncing from that point only and not from the beginning with each time
the node is restarted.

```ruby
require 'kinesis/consumer'
require 'aws-sdk-dynamodb'

dynamo_client = Aws::DynamoDB::Client.new

Kinesis::Consumer.new(
  stream_name: 'your-stream-here',
  dynamodb: { client: dynamo_client, table_name: 'kinesis-state-test', consumer_group: 'my-app-name' }
)
```

#### Producer

The producer works by launching a single thread for publishing to the stream

```ruby
require 'kinesis/producer'

producer = Kinesis::Producer.new(stream_name: 'your-stream-here')
producer.put({ a: 1, b: 2, c: 3 })
```

By default the accumulation buffer time is 500ms, or the max record size of 1Mb, whichever occurs first.  You can
change the buffer time when you instantiate the producer via the ``buffer_time`` kwarg, specified in seconds.  For
example, if your primary concern is budget and not performance you could accumulate over a 60 second duration.

```ruby
producer = Kinesis::Producer.new(stream_name: 'your-stream-here', buffer_time: 60)
```

WARNING: Sending `kill -9` to the producer process when it's not finished flushing the records will result in lost messages

#### AWS Permissions
By default, the producer, consumer & state classes all use the default [Ruby AWS SDK credentials chain](https://docs.aws.amazon.com/sdk-for-ruby/v3/developer-guide/setup-config.html).

Alternatively, you can pass in the clients (depends if there is support for it) like so:

```
require 'kinesis/consumer'

client = Aws::Kinesis::Client.new(access_key_id: 'something_here', secret_access_key: 'something_here')
consumer = Kinesis::Consumer.new(stream_name: 'your-stream-here', kinesis: { client: client })
```

## Enhanced Fan-Out Support

This library supports Kinesis Enhanced Fan-Out, which requires:

- aws-sdk-kinesis gem version 1.10.0 or later
- Ruby version 2.1 or later
- http-2 gem version 0.10 or later

Enhanced Fan-Out uses the `Aws::Kinesis::AsyncClient` for HTTP/2 streaming instead of the regular `Client`. This is automatically handled by the library.

```
