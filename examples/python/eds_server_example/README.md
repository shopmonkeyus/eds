<!-- markdownlint-disable-file MD024 MD025 MD041 -->

![shopmonkey!](https://www.shopmonkey.io/static/sm-light-logo-2c92d57bf5d188bb44c1b29353579e1f.svg)

# Overview

This Python example serves as a quick  data code example for users of Enterprise Data Streaming looking to filter and trigger third party workflows when data is received. This is not a comprehensive example but should provide the reader with enough to 
1. Connect to NATS through the nats client package nats-py. 
2. Read from a NATS Jetstream stream and react to data. 
3. Run the eventloop to keep the connection open while waiting for new messages. 


## Requirements

This example repository is requires Python3.7+ (For )

## Basic Usage
While EDS is running with the --embed-nats option: 
```
pip install -r requirements.txt
python eds_server_example/eds_server_example.py \
            --nats_host <HOST URL> \
            --stream <The stream you would like to subscribe to> \
            --consumer_group <A durable consumer group so you can recover after a crash>
```
The defaults are listed in the script and should work out of the box as an example so you could just run `python eds_server_example/eds_server_example.py` if you are running everything with defaults. 

## License

All files in this repository are licensed under the [MIT license](https://opensource.org/licenses/MIT). See the [LICENSE](./LICENSE) file for details.
