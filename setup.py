from distutils.core import setup

setup(
  name='kafka-ibus-streaming-batch-consumer',
  version='0.1.0',
  author='Lingyu Zhou',
  author_email='zhoulytwin@gmail.com',
  scripts=['bin/kafka-ibus-streaming-batch-consumer.py'],
  url='https://github.com/TwistTRL/kafka-ibus-streaming-batch-consumer',
  license='GPL-3.0',
  description='https://github.com/TwistTRL/kafka-ibus-streaming-batch-consumer',
  install_requires=[
    "docopt >= 0.6.1",
    "kafka-python >= 1.4.6"
  ],
)
