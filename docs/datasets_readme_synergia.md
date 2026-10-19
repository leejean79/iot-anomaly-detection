# Multi-sensor, Multi-device Environmental Indoor Dataset of a Smart Building #

Introduction

SYNERGIA, funded by Innovate UK, aimed to improve privacy and reduce latency in IoT platforms by enhancing the security and resilience of industrial IoT devices, enabling the development of future networks. It moves some computation to the Edge to address privacy and scalability issues of the current cloud-based IoT platforms.

During the project, to collect real-world data, we deployed an end-to-end IoT network in an office used by research staff at the University of Bristol. The office is actively used by a significant number of academic personnel and students (max occupancy of 28 people). It gets exposed to environmental changes such as seasonal temperature, humidity, and light fluctuations. The endpoints are located in different locations in the lab to collect varying data due to differentiation between the areas. The network consists of eight stationary, severely resource-constrained IoT endpoints, an additional device called Umbrella acting as the “edge”, and a server for data collection and controlling the experiment.

Each IoT endpoint hosts sensors providing temperature, humidity, pressure, gas, accelerometer, and light readings. We collected two additional pieces of information: the measurements’ accuracy value, calculated by the environmental sensors and the received signal strength indicator(RSSI). The data was acquired using several sensors in a smart building/office environment. The sensors were integrated to an IoT Nordic nRF52840 DK board endpoint is as following:

(1) "ISL29125" Light Sensors: Collects intensity of the light.

(2) "MMA8452Q" Accelerometer Sensors.

(3) "BME680" Environmental Digital Sensors: Comprise of gas (VOC/ CO₂), pressure, temperature and humidity sensors.

The experiment started in February 2022. We stored over six months (February - September 2022) of sensor readings for experimental reasons, in CSV file format.

This repository provides the following.

Raw environmental sensor data from a deployment in an indoor office area.

#Location
The dataset was collected in University of Bristol.

# Data Description
The data is organised in CSV files. The CSV files are comma-separated. Each row corresponds to a sensor reading and contains four values:

* Time: The timestamp of the sensor reading (Unix Epoch timestamp format: number of seconds that have elapsed since 00:00:00 UTC on 1 January 1970)
* DeviceId: A pseudo device identifier (A, B, ... H)
* Sensor: The type of reading. Can take on of the following values:
  * Temperature
  * Humidity
  * Pressure
  * Gas
  * Accelerometer
  * Light
  * MIC
  * RSSI
* Value. This has a different meaning depending on the content of "Sensor":
  * Temperature: Environmental temperature reading in degrees Celcius(C)
  * Humidity: Environmental humidity in Percentage (%)
  * Pressure: Barometric pressure in Hectopascal(hPa)
  * Gas: Indoor air quality (IAQ), more details about the IAQ value can be found in BME60 sensor datasheet.
  * Accelerometer: Acceleration in "g" 
  * Light: Light intensity in  lumen per square metre(Lux)
  * MIC: A number provided by the BME60 sensor, indicating the quality of the measurement.
  * RSSI: Signal strength indicator calculated from the device's radio transceiver. Relative index, unitless.

#Usage
The data can be processed by any data processing tool such as Python. In case of processing the data with Python, following libraries can bu useful such as pandas, scikit-learn, numpy, tensorflow/keras, matplotlib.

The folder contains CVS files versions of the dataset.

#Licence
This work was supported by UK Research and Innovation, Innovate UK [grant number 53707]. SYNERGIA - Secure by design end to end platform for large scale resource constrained IoT Applications. InnovateUK


