# CH 01. Spark Inroduction and Environment Setup

## 1.1 What is Spark

## 1.2 Spark Components

## 1.3 Spark Program Flow

## 1.4 Spark Ecosystem

## 1.5 Setting Up the Spark-in-Action VM

The VM consists of the following software stack:

	64-bit Ubuntu OS, 14.04.4 (nicknamed Trusty)</br>
	Java 8 (OpenJDK)</br>
	Hadoop 2.7.2</br>
	Spark 2.0</br>
	Kafka 0.8.2</br>
	
Install VirtualBox and Vagrant:

	VirtualBox: www.virtualbox.org</br>
	Vagrant: www.vagrantup.com/downloads.html</br>
	
Download Vagrant box metadata json:
	
~~~shell
mkdir ~/Documents/sparkvm/
cd ~/Documents/sparkvm/
wget https://raw.githubusercontent.com/spark-in-action/first-edition/master/spark-in-action-box.json
~~~ 

Download the VM (5 GB) and reigist it to Vagrant:

~~~shell
cd ~/Documents/sparkvm/
vagrant box add spark-in-action-box.json
~~~

Initialize the Vagrant VM

~~~shell
cd ~/Documents/sparkvm/
vagrant init manning/spark-in-action
~~~

Start the VM 

> If being asked about the network interfaces selection, select the one with internet access

~~~shell
vagrant up
~~~
~~~
Bringing machine 'default' up with 'virtualbox' provider...
==> default: Checking if box 'manning/spark-in-action' is up to date...
==> default: Clearing any previously set forwarded ports...
==> default: Clearing any previously set network interfaces...
...
==> default: Available bridged network interfaces:
1) 1x1 11b/g/n Wireless LAN PCI Express Half Mini Card Adapter
2) Cisco Systems VPN Adapter for 64-bit Windows
==> default: When choosing an interface, it is usually the one that is
==> default: being used to connect to the internet.
    default: Which interface should the network bridge to? 1
==> default: Preparing network interfaces based on configuration...
...
~~~

Stop the VM 

~~~shell
vagrant halt
~~~

Destroy the VM (if needed)

> it will stop the machine but preserve your work. If you wish to completely remove the VM and free up its space

~~~
vagrant destroy
~~~

The downloaded Vagrant box also can be removed 

~~~
$ vagrant box remove manning/spark-in-action
~~~


## 1.6 Summary

* Apache Spark is an exciting new technology that is rapidly superseding Hadoop’s MapReduce as the preferred big data processing platform.
* Spark programs can be 100 times faster than their MapReduce counterparts.
* Spark supports the Java, Scala, Python, and R languages.
* Writing distributed programs with Spark is similar to writing local Java, Scala, or Python programs.
* Spark provides a unifying platform for batch programming, real-time data-processing functions, SQL-like handling of structured data, graph algorithms, and machine learning, all in a single framework.
* Spark isn’t appropriate for small datasets, nor should you use it for OLTP applications.
* The main Spark components are Spark Core, Spark SQL, Spark Streaming, Spark MLlib, and Spark GraphX.
RDDs are Spark’s abstraction of distributed collections.
* Spark supersedes some of the tools in the Hadoop ecosystem.
* You’ll use the spark-in-action VM to run the examples in this book.


