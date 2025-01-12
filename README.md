

<img src="https://raw.githubusercontent.com/Blueratzxk/Accordion_engine/master/imgs/icon.png" width="20%" />

# Accordion: cloud-native data analysis in accordance with your mind

The first IQRE (Intra-Query Runtime Elasticity) SQL query engine (prototype).

* Accordion can tune the parallelism of a query at any time during its execution, and the tuning process will not affect the query execution.
* Its what-if service facilitates parallelism tuning and can predict DOP-tuned remaining query execution times for users. 
* Accordion can be used to solve the problem of cost-performance tradeoffs when analyzing data on the cloud. 
* It also can be used to build a fully serverless cloud-native database or data warehouse.

**Here is a demo:**

![image](https://raw.githubusercontent.com/Blueratzxk/Accordion_engine/master/imgs/UII.gif)  

# Prerequisites

* Linux Ubuntu version v20.04, v22.04, v24.04.

* Make sure you have installed CMake version 3.25 (or newer) on your system. 
 
* GCC version 11 (or newer).



# Dependencies

Accordion has the following third-party dependencies

* [@apache arrow](https://github.com/apache/arrow) : Data format exchanging between operators and tasks.

* [@pistache](https://github.com/pistacheio/pistache) : Restful HTTP server.


* [@nlnohamn json](https://github.com/nlohmann/json) : Json library.

* [@nlnohamn json fifomap](https://github.com/nlohmann/fifo_map) : Json library.

* [@tbb](https://github.com/oneapi-src/oneTBB) : TBB library.

* [@spdlog](https://github.com/gabime/spdlog) : Log library.



# Building from source

**1. Get Accordion source code from Git Hub.**
```
$ git clone https://github.com/Blueratzxk/Accordion_engine
$ cd Accordion_engine/
```

**2. Install all the dependencies.**

   
* The source file `fifo_map.hpp` needs to be copied to `json/include/nlohmann/`.
* After installing all the dependent libraries, it is necessary to modify the `Accordion_engine/CMakeLists.txt` to allow CMake to find all the header files and dynamic link libraries (according to the locations of installed dependencies). Here is an example.
```
include_directories(
        "/home/zxk/dependency/json/include/"
        "/usr/local/include/"
        "/home/zxk/dependency/oneapi-tbb-2021.9.0/include"
)

target_link_libraries(
        Accordion
        pistache
        /usr/local/lib64/libpistache.so
        curl
        arrow
        arrow_acero
        arrow_flight
        gandiva
        libtbb
)
```


**3. Build Accordion and copy the Accordion execution file to `Accordion_engine/accordion/`.**
```
$ cd Accordion_engine/
$ mkdir build
$ cd build
$ cmake ..
$ make -j6
$ cp Accordion ../accordion/
```


**4. Build Accordion tools and copy them to `Accordion_engine/accordion/`.**
```
$ cd accordion_tools/DFSMaker
$ mkdir build && cd build
$ cmake .. && make -j6
$ cp DFSMaker ../../../accordion/dataSet/

$ cd accordion_tools/partitionsMaker
$ mkdir build && cd build
$ cmake .. && make -j6
$ cp partitionsMaker ../../../accordion/dataSet/

$ cd accordion_tools/httpConfigIpUpdater
$ mkdir build && cd build
$ cmake .. && make -j6
$ cp httpConfigIpUpdater ../../../accordion/sbin/
```


**5. Move `Accordion_engine/accordion/` to the home directory and check if the Accordion can run.**
```
$ cp -r Accordion_engine/accordion/ ~/
$ cd ~/accordion/
$ bash run.sh
```


# Running Accordion stand-alone
 
```
$ cd ~/accordion/
```
**1. Configure the Accordion.**
* Modify the `httpconfig.config` file.
Change all IP addresses in the file to the host IP address. Here is an example.

```
{
    "coordinator":{
        "Restful_Web_Server_IP":"localhost",
        "Restful_Web_Server_Port":"9080",
        "Arrow_RPC_Server_IP":"localhost",
        "Arrow_RPC_Server_Port":"9081"
    },
    "local":{
        "Restful_Web_Server_IP":"localhost",
        "Restful_Web_Server_Port":"9080",
        "Arrow_RPC_Server_IP":"localhost",
        "Arrow_RPC_Server_Port":"9081"
    },
    "nic":"ens33",
    "HttpServerAddress":"192.168.226.137:9080"
}
```
* Add workers.
```
$ echo "localhost" > sbin/slaves
```
* Configure the workers' user and password
```
$ echo -e "root\nroot" >  userpasswd
```

**2. Import TPC-H dataSet.**

* Get dbgen tools from [TPC-H](https://www.tpc.org/tpch/).
* Generate TPC-H tables and copy them to `accordion/dataSet/`. Below is an example.
```
$ cd dataSet
$ ls
customer.tbl  makeDFS.sh       PartitionsMaker.sh  scpFile.sh
DFSMaker      nation.tbl       partsupp.tbl        supplier.tbl
DFSMaker.sh   orders.tbl       part.tbl            tablePartitions.txt
lineitem.tbl  partitionsMaker  region.tbl

```
* Modify the `tablePartitions.txt`. The first column is the table name, the second column is the number of storage nodes (how many nodes we want to distribute this table to store), and the third column is the number of table partitions contained in each storage node. Below is an example. Since we are running Accordion on a single machine, there is only 1 storage node. This configuration slices the ORDERS table and LINEITEM table horizontally into 4 partitions.
```
nation,1,1
supplier,1,1
region,1,1
part,1,1
partsupp,1,1
customer,1,1
orders,1,4
lineitem,1,4
```
* Generate the `DataFileDicts`. This file contains information about the storage configuration, schema, etc. for each table.
```
$ bash PartitionsMaker.sh
```

* Overwrite the DataFileDict file in the accordion directory.
```
cp DataFileDicts.out ../DataFileDicts
```

* Generate partitions for each table.
```
$ bash DFSMaker.sh
$ ls
customer.tbl       lineitem.tbl_1  orders.tbl_0        partsupp.tbl
DataFileDicts.out  lineitem.tbl_2  orders.tbl_1        part.tbl
DFSMaker           lineitem.tbl_3  orders.tbl_2        region.tbl
DFSMaker.sh        makeDFS.sh      orders.tbl_3        scpFile.sh
lineitem.tbl       nation.tbl      partitionsMaker     supplier.tbl
lineitem.tbl_0     orders.tbl      PartitionsMaker.sh  tablePartitions.txt
```
* Move these partitions to `accordion/data/`
```
$ bash makeDFS.sh
```

**3. Run TPC-H queries.**
* Run Accordion.
```
$ cd ..
$ bash run.sh
```
* The WEB UI of Accordion can be accessed through IP:9082 (for example, 192.168.226.137:9082). Here is an example.
 
![image](https://raw.githubusercontent.com/Blueratzxk/Accordion_engine/master/imgs/UI1.png)

* Enter "run Q1S" in the left input box to run TPC-H Q1 (stand-alone version). There are 12 TPC-H queries in Accordion (Q1S ~ Q12S).

![image](https://raw.githubusercontent.com/Blueratzxk/Accordion_engine/master/imgs/UI2.png)

* Click the `Controller` button to enter the query control panel, you can adjust the query stage parallelism and task parallelism.

![image](https://raw.githubusercontent.com/Blueratzxk/Accordion_engine/master/imgs/UI31.png) 

* Click the `Complete` button to get the results of the query execution. 

![image](https://raw.githubusercontent.com/Blueratzxk/Accordion_engine/master/imgs/UI4.png)  


# Deploying Accordion on the cloud (or in a distributed environment)

Suppose there are 5 nodes, `192.168.0.121`, `192.168.0.122`, `192.168.0.123`, `192.168.0.124`, `192.168.0.125`.

Where `192.168.0.121` is the coordinator. And `192.168.0.122, 192.168.0.123, 192.168.0.124, and 192.168.0.125` are workers. Inside the workers, `192.168.0.122 and 192.168.0.123` are storage nodes, and `192.168.0.124 and 192.168.0.125` are compute nodes.

**1. Configure a distributed Accordion.**
* Copy the `accordion/` directory to the `XXX@192.168.0.121:~/` (The `accordion/` must be copied to a home directory).
* Modify the `httpconfig.config` file.
Change all IP addresses in the file to the host IP address.
```
{
    "coordinator":{
        "Restful_Web_Server_IP":"192.168.0.121",
        "Restful_Web_Server_Port":"9080",
        "Arrow_RPC_Server_IP":"192.168.0.121",
        "Arrow_RPC_Server_Port":"9081"
    },
    "local":{
        "Restful_Web_Server_IP":"192.168.0.121",
        "Restful_Web_Server_Port":"9080",
        "Arrow_RPC_Server_IP":"192.168.0.121",
        "Arrow_RPC_Server_Port":"9081"
    },
    "nic":"eno3",
    "HttpServerAddress":"192.168.0.121:9080"
}
```
* Add workers. (Does not contain the coordinator IP)
```
$ echo -e "192.168.0.122\n192.168.0.123\n192.168.0.124\n192.168.0.125" > sbin/slaves
```

* Configure the workers' user and password
```
$ echo -e "root\nroot" >  userpasswd
```
* Configure SSH passwordless login.
```
$ bash sbin/batchSendKey.sh
```
* Copy accordion/ to each worker.
```
$ bash sbin/deployAccordion.sh
```
* Update httpconfig.config for each worker.
```
$ bash sbin/updateAllLocalIp.sh
```

**2. Import TPC-H dataSet.**

* Get dbgen tools from [TPC-H](https://www.tpc.org/tpch/).
* Generate TPC-H tables (`CSV format`) and copy them to `accordion/dataSet/`. Below is an example.
```
$ cd dataSet
$ ls
customer.tbl  makeDFS.sh       PartitionsMaker.sh  scpFile.sh
DFSMaker      nation.tbl       partsupp.tbl        supplier.tbl
DFSMaker.sh   orders.tbl       part.tbl            tablePartitions.txt
lineitem.tbl  partitionsMaker  region.tbl

```
* Modify the `tablePartitions.txt`. The first column is the table name, the second column is the number of storage nodes (how many nodes we want to distribute this table to store), and the third column is the number of table partitions contained in each storage node. Below is an example. This configuration slices the ORDERS table and LINEITEM table horizontally into 4 partitions.
```
nation,1,1
supplier,1,1
region,1,1
part,1,1
partsupp,1,1
customer,1,1
orders,2,2
lineitem,2,2
```
* Generate the `DataFileDicts`. This file contains information about the storage configuration, schema, etc. for each table.
```
$ bash PartitionsMaker.sh
```

* Overwrite the DataFileDict file in the accordion directory.
```
$ cp DataFileDicts.out ../DataFileDicts
```

* Generate partitions for each table.
```
$ bash DFSMaker.sh
$ ls
customer.tbl       lineitem.tbl_1  orders.tbl_0        partsupp.tbl
DataFileDicts.out  lineitem.tbl_2  orders.tbl_1        part.tbl
DFSMaker           lineitem.tbl_3  orders.tbl_2        region.tbl
DFSMaker.sh        makeDFS.sh      orders.tbl_3        scpFile.sh
lineitem.tbl       nation.tbl      partitionsMaker     supplier.tbl
lineitem.tbl_0     orders.tbl      PartitionsMaker.sh  tablePartitions.txt
```
* Distribute these partitions to each storage worker.
```
$ bash makeDFS.sh
```

* Copy the `DataFileDicts` to each worker.
```
$ cd ..
$ bash copy.sh DataFileDicts
```
**3. Run TPC-H queries on the Accordion cluster.**
* Start the Accordion cluster.
```
$ bash startcluster.sh
```
* The WEB UI of Accordion can be accessed through IP:9082 (for example, 192.168.0.121:9082). Then you can run the queries as mentioned before.
* Enter "run Q1" in the left input box to run TPC-H Q1 (distributed version). There are 12 TPC-H queries in Accordion (Q1 ~ Q12).
* Stop the Accordion cluster.
```
$ ctrl ^c
$ bash stopcluster.sh
```
**4. Notes.**
* Dependencies. When deploying Accordion to the cloud, a `libs` directory needs to be added to the `accordion/` directory. The `libs` directory needs to hold all the dynamic link libraries that the Accordion executable file relies on (these link library files can be collected from the machine where Accordion is compiled).
* Script. You can run a query by modifying a script file in the `accordion/` directory. You can trigger script execution by typing the 's' character in Accordion's command terminal (not the Web UI). You can also enter the 'h' character to view and use some of Accordion's commands. Queries run via script or web UI have their runtime information collected in the `accordion/monitorData/` directory.
* Execution Configuration. The `accordion/execution.config` file is used to configure the initial intra-task or intra-stage parallelism of the query execution.





