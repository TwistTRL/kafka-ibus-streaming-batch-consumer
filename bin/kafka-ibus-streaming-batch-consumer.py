#!/usr/bin/env python
"""
Usage:
  script.py <kafkaHost> <kafkaPort> <tcpHost> <tcpPort> <groupId> <topic> <logTopic> <interval>
"""
from docopt import docopt
import asyncio
from datetime import datetime
from kafka.consumer import KafkaConsumer
from kafka.producer import KafkaProducer
from kafka import TopicPartition

class IBUSStreamingDownsamplingConsumer:
  LOG_FORMAT ="{} UTC_TS\t"\
              "{}"

  def __init__(self,kafkaHost,kafkaPort,tcpHost,tcpPort,group_id,topic,logTopic,interval):
    self.kafkaHost = kafkaHost
    self.kafkaPort = kafkaPort
    self.tcpHost = tcpHost
    self.tcpPort = tcpPort
    self.group_id = group_id
    self.topic = topic
    self.logTopic = logTopic
    self.interval = int(interval)
    self.consumer = KafkaConsumer(topic,
                                  bootstrap_servers=["{}:{}".format(kafkaHost,kafkaPort)],
                                  group_id=group_id,
                                  enable_auto_commit=False)
    self.producer = KafkaProducer(bootstrap_servers=["{}:{}".format(kafkaHost,kafkaPort)])
    self.tcpWriter = None

  def getTopicPartitions(self):
    self.consumer.topics()                                              #This ensures local cache is updated with
                                                                        # information about partitions, offsets etc.
    pids = self.consumer.partitions_for_topic(self.topic)
    tps = [TopicPartition(self.topic,pid) for pid in pids]
    return tps

  def getTopicPartitionsCommittedPositions(self):
    tps = self.getTopicPartitions()
    ret = [(tp,self.consumer.committed(tp)) for tp in tps]
    return ret

  async def tcp_server_handler(self,reader,writer):
    addr = str(writer.get_extra_info("socket").getpeername())
    if self.tcpWriter is not None:
      self.log("refused "+addr)
      writer.write(b"Connection limit reached; connection refused.")
      writer.close()
      return
    self.log("accepted "+addr)
    self.tcpWriter = writer
    t1 = asyncio.create_task(self.poll_from_Kafka(writer))
    try:
      while True:
        data = await reader.read(1)                                     # 1024*16 bytes
        if not data:
          break
    except BrokenPipeError:
      """
      Catches connecton reset by peer when we are sending the batched data,
       which is also when we cannot check for reader. The broken connection
       on the writer side will ultimately lead to  BrokenPipeError on the
       reader side. Hence
      """
      pass
    finally:
      t1.cancel()
      self.log("closed "+addr)
      writer.close()
      self.tcpWriter = None

  async def poll_from_Kafka(self,writer):
    while True:
      prevPos = self.getTopicPartitionsCommittedPositions()
      polled = self.consumer.poll(timeout_ms=1000)
      records = [record.value for recordList in polled.values() for record in recordList]
      try:
        for record in records:
          writer.write(record)
          await writer.drain()
      except ConnectionResetError:
        """
        The error is not thrown reliably. If a connection is broken, and
         one try to
            writer.write(record)
            await writer.drain()
         This error may not manifest. It is thrown more often when one try
         to repeatedly write to and drain a broken connection.
        """
        print("Last batch not fully sent, not commited.")
        for tp,pos in prevPos:
          self.consumer.seek(tp,pos)
        break
      else:
        self.consumer.commit()
      await asyncio.sleep(self.interval)

  def log(self,msg):
    self.producer.send( self.logTopic,
                        self.LOG_FORMAT.format( datetime.now().timestamp(),
                                                msg
                                                ) \
                            .encode()
                        )

  def cleanup(self):
    self.log("shutdown")
    self.consumer.close()
    self.producer.flush()
    self.producer.close()
  
  def run(self):
    self.log("running")
    asyncio.run(self._async_run())
  
  async def _async_run(self):
    tcpServer = await asyncio.start_server(self.tcp_server_handler, self.tcpHost, self.tcpPort)
    await tcpServer.serve_forever()
  
def main():
  options = docopt(__doc__)
  group_id = "zly"
  topic = "test"
  tcpHost = options["<tcpHost>"]
  tcpPort = options["<tcpPort>"]
  kafkaHost = options["<kafkaHost>"]
  kafkaPort = options["<kafkaPort>"]
  group_id = options["<groupId>"]
  topic = options["<topic>"]
  logTopic = options["<logTopic>"]
  interval = options["<interval>"]
  consumer = IBUSStreamingDownsamplingConsumer( kafkaHost,kafkaPort,
                                                tcpHost,tcpPort,
                                                group_id,topic,logTopic,
                                                interval)
  try:
    consumer.run()
  except KeyboardInterrupt:
    consumer.cleanup()

if __name__=="__main__":
  main()
