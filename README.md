
# Distributed Node-red Flows


## Assignment

Note: for more details on the assignment see [Projects 2021.pdf](https://github.com/Furcanzo/nodered-kafka-middleware-project/blob/master/Projects%202021.pdf)

#### Description of the project

Implement an architecture that allows Node-red flows to span multiple devices.

Normally, a Node-red flow executes locally to the machine where it is installed.
Instead, consider multiple Node-red installations that
i) register to a central repository that maintains information on all running installations and
ii) can exchange messages among them by logically connecting the output of a node in one installation to the input of another node in a different installation.

Demonstrate that a flow developed to be executed on a single Node-red installation may be split across two or multiple Node-red machines with a limited set of modifications to the flow itself.

#### Assumptions and Guidelines

* The set of running Node-red installations is dynamic, that is, the implementation must be able to manage Node-red installations coming and going while the application executes.
* The solution must be agnostic to the content of messages, that is, it must be usable regardless of the format of messages or the nature of nodes that produce/consume them.


## Usage

Run Kafka:

* `cd $KAFKA_FOLDER_PATH`
* `./bin/zookeeper-server-start.sh config/zookeeper.properties`
* `./bin/kafka-server-start.sh config/server.properties`

Run Node-red:

* `cd $PROJECT_FOLDER_PATH`
* Modify the `address.txt` file with the correct address of the Kafka server.
* `node-red -p 1880`
* `node-red -p 1881`
* Import on the two node-red instances (by using their web server interface) the `consumer.json` and `producer.json` files respectively.
* Deploy and run.


## Developers

[Accordi Gianmarco](https://github.com/gianfi12)

[Buratti Roberto](https://github.com/Furcanzo)

[Motta Dennis](https://github.com/Desno365)
