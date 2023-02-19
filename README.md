<span style="color: #f2cf4a; font-family: Babas; font-size: 2em;">INSPIRATION DAY</span>

# Outbound Large Shipment Alert

<!-- ABOUT THE PROJECT -->
## About The Project

Large Shipment Alert (LSA) is Amazon ChimeBot scheduled to alert of shipments greater that a trigger appears on backlog.

Here's why:
* Large shipment are likely to cause CX impact
* Problem Solving tools (POPS) and PackApp are not designed to process such shipments with ease
* Shipment cycle time is greater than regular shpments

LSA is meant to run on AWS Lambda in order not to depend on personal virtual machines

<!-- GETTING STARTED -->
## Usage

When LSA is triggered a messega is posted tagging all present members on chime room. Message contains following info:

* Shipment ID (linked to Rodeo)
* ASIN
* Shipment quantity
* Expected Ship Date

![LSA_Bot](https://github.com/KMN43/lambda_LargeShipment/blob/main/LSA_Bot.JPG?raw=true)


<!-- ROADMAP -->
## Roadmap

- [x] Generate Alert message
- [x] Embebed libk to shipment on Rodeo
- [ ] Multi FC Alert
- [ ] Multi-level Alert
    - [ ] Alert L.1 - When shipments are within next ExSD SLA
    - [ ] Alert L.2 - when shipments are outside it's ExSD SLA


<!-- CONTACT -->
## POC

Jorge Casas (DELGJR) - [Mail](delgjr@amazon.com)

Project Link: [LSA_Project](https://github.com/KMN43/lambda_LargeShipment)



