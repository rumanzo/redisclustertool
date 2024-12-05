
# Redisclustertool
## Description

Redisclustertool is a tool for monitoring redis cluster map for misconfigurations and level out masters across all servers or group servers (per datacenter for example)
Has two classes, default and datacenter. For default just run redisclustertool.py. Datacenter use described below

- Has checks:
  - master and replica places on same node (datacenter)
  - replicas not spread across the cluster (for example all replicas stored on one server or datacenter)
  - Min-max masters across nodes more than defined percent (datacenters)
  - Min-max masters in datacenter more than defined percent if their more than one server in it (datacenter only)
  - Master doesn't have enough replicas on other servers (datacenters). Counted automatically or defined with arg --replicas.
  - Master doesn't have replicas on another nodes (datacenters)


- Features:
- Level out masters and replicas across cluster respectfully for fault tolerance

- Can be used with monitoring as script with --dry-run arg. Return code exits
  - 0 OK
  - 1 WARN level out problems
  - 2 CRIT problems with data loss possibility (master and replica on the same server for example).


- With standard run propose all actions after analyze and ask for agreement, then start gracefully make failovers and replicates with default timeout 90s.

- With parameter reduce level out will not use ports higher than defined. It moves masters and slaves to port lower than defined with reduce. Should be used with --replicas arg. Can be useful for reconfigure cluster, make cluster wider with same number of masters and slaves.
For example,  you have cluster with 3 servers and redis services on ports 6700-6710 (30 redis services at all), and redisclutertools runs with arg --reduce 6708, masters level out at first on ports 6700-6708, and then replicas attaches for them will all described above rules. And after you can make cluster forget all on redis services on ports 6709-6710 (replicas)

- If redis nodes (each instance) can't be spread across cluster respectfully all rules - redisclustertool print error, try to define lower replica number or fix you configuration

### Install
- python 3.6+
- redis-py
 
```
pip -r requirements.txt
```


### Help:
```
usage: redisclustertool.py [-h] [--host HOST] [--port PORT] [--password PASSWORD] [--reduce REDUCE] [--replicas REPLICAS] [--skew SKEW] [--group-skew GROUP_SKEW] [--timeout TIMEOUT] [--fix-only] [--force] [--alive-only] [--credentials CREDENTIALS] [--simple] [--use_v1] [--noslots_ok] [--dry-run] [--nagios]
   [--save-nodes SAVE_NODES | --load-nodes LOAD_NODES]

redis cluster node print helper

options:
  -h, --help            show this help message and exit

connect:
  --host HOST, -c HOST  host to connect for
  --port PORT, -p PORT  port to connect for
  --password PASSWORD   redis password

optional:
  --reduce REDUCE, -r REDUCE
                        reduce port till number. If cluster has port 7200:7220 and parameter defined to 7210, than program will reduce all port higher than 7210 and levelout masters and replicas on port lower
  --replicas REPLICAS   desired number of replicas
  --skew SKEW, -s SKEW  desired master count percentage difference per datacenter
  --group-skew GROUP_SKEW, -g GROUP_SKEW
                        desired master count percentage difference per server in datacenter
  --timeout TIMEOUT, -t TIMEOUT
                        timeout between operations
  --fix-only            Only fix problems, skip rebalance
  --force               Force rebalance iteration
  --alive-only          Use only connected nodes
  --credentials CREDENTIALS
                        credential config file
  --simple              Do not use datacenter detection functionality with inventory
  --noslots_ok          Still rebalance despite having masters without slots

monitoring:
  --dry-run             Only print current distribution problems
  --nagios              Print short message for nagios short line

debug:
  --save-nodes SAVE_NODES
                        save original nodes objects in json file
  --load-nodes LOAD_NODES
                        load original nodes objects from json file
```



> [!WARN]
> Define timout less than 90s can lead to errors with tool or cluster health due high load on cluster and time needed for elections.

## redisclustertool.py debug
For local develop and bugreports it is possible to save snapshot of nodes with arg --save-nodes somename.json and run with --load-nodes without any connections locally.


# Examples
## Check redis cluster
```
[root@server1 ~]$ redisclustertool.py -p 6771 --dry-run
Processing with replica count 2 and use port 6771
Now cluster has instances per group:
    Server 10.0.89.71 (has 33.33% masters): (masters: 2   slaves: 4  )
    Server 10.0.89.72 (has 33.33% masters): (masters: 2   slaves: 4  )
    Server 10.0.89.76 (has 33.33% masters): (masters: 2   slaves: 4  )
Skew is 0.0%


And has problems:
    None
[root@server1 ~]$ echo $?
0
```
Then I've broken configuration
```
[root@server1 ~]$ redis-cli -h 10.0.89.72 -p 6773 cluster replicate 080dddce00b1e0a8031e93b98a87a476ab02c6eb
OK
[root@server1 ~]$ redis-cli -h 10.0.89.72 -p 6774 cluster replicate 080dddce00b1e0a8031e93b98a87a476ab02c6eb
OK
```
And got
```
[root@server1 ~]$ redisclustertool.py -p 6771 --dry-run
Processing with replica count 2 and use port 6771
Now cluster has instances per group:
    Server 10.0.89.71 (has 33.33% masters): (masters: 2   slaves: 4  )
    Server 10.0.89.72 (has 33.33% masters): (masters: 2   slaves: 4  )
    Server 10.0.89.76 (has 33.33% masters): (masters: 2   slaves: 4  )
Skew is 0.0%


And has problems:
Masters don't have desired replica count 2 problem (2):
    Master node 5870cfef30a39b92471ba4dfd035966851aa5ecc (10.0.89.71) has 1 replicas
    Master node e1b8bdf9db261ec5cd660d0463f9352647497770 (10.0.89.76) has 1 replicas

[root@server1 ~]$ echo $?
2
```

## Level out with fix problem
```
[root@server1 ~]$ ./redisclustertool.py -p 6771 -t 90
Processing with replica count 2 and use port 6771
Now cluster has instances per group:
    Server 10.0.89.71 (has 33.33% masters): (masters: 2   slaves: 4  )
    Server 10.0.89.72 (has 33.33% masters): (masters: 2   slaves: 4  )
    Server 10.0.89.76 (has 33.33% masters): (masters: 2   slaves: 4  )
Skew is 0.0%


And has problems:
Masters don't have desired replica count 2 problem (2):
    Master node 5870cfef30a39b92471ba4dfd035966851aa5ecc (10.0.89.71) has 1 replicas
    Master node e1b8bdf9db261ec5cd660d0463f9352647497770 (10.0.89.76) has 1 replicas

Printing new plan:
Attach slave  2c15b9b3ed26469129ccc21d3189aa3fbc35cc26 10.0.89.72:6773 to 5870cfef30a39b92471ba4dfd035966851aa5ecc 10.0.89.71:6772
Attach slave  946eee32462d65d2b99b215469b80f0dfa409d08 10.0.89.76:6775 to 5870cfef30a39b92471ba4dfd035966851aa5ecc 10.0.89.71:6772
Attach slave  28d355544294ba91e3c1ce1f3680e5a23506cb2e 10.0.89.71:6773 to e1b8bdf9db261ec5cd660d0463f9352647497770 10.0.89.76:6771
Attach slave  c331ed5c1700f8edf8747925c4184bf8ea49d918 10.0.89.72:6774 to e1b8bdf9db261ec5cd660d0463f9352647497770 10.0.89.76:6771

It will take 4 iterations with timeout 90 and will take 0:06:00 time

Cluster will have instances per group:
    Server 10.0.89.71 (has 33.33% masters): (masters: 2   slaves: 4  )
    Server 10.0.89.72 (has 33.33% masters): (masters: 2   slaves: 4  )
    Server 10.0.89.76 (has 33.33% masters): (masters: 2   slaves: 4  )
Skew is 0.0%


And will have problems
    None

Proceed plan to execute with timeout 90 seconds between operations? y/n
y
Will be finished at 2024-12-02 21:49
Attach slave  2c15b9b3ed26469129ccc21d3189aa3fbc35cc26 10.0.89.72:6773 to 5870cfef30a39b92471ba4dfd035966851aa5ecc 10.0.89.71:6772
Cluster answer: True
Attach slave  946eee32462d65d2b99b215469b80f0dfa409d08 10.0.89.76:6775 to 5870cfef30a39b92471ba4dfd035966851aa5ecc 10.0.89.71:6772
Cluster answer: True
Attach slave  28d355544294ba91e3c1ce1f3680e5a23506cb2e 10.0.89.71:6773 to e1b8bdf9db261ec5cd660d0463f9352647497770 10.0.89.76:6771
Cluster answer: True
Attach slave  c331ed5c1700f8edf8747925c4184bf8ea49d918 10.0.89.72:6774 to e1b8bdf9db261ec5cd660d0463f9352647497770 10.0.89.76:6771
Cluster answer: True

```

## Reduce nodes
Move all master nodes to lower ports. My cluster spread across 3 servers with ports 6771:6776 with 2 replicas. Let's shrink it
```
[root@server1 ~]$ ./redisclustertool.py -p 6771 --replicas 1 --reduce 6774
Processing with replica count 1 and use port 6771
Now cluster has instances per group:
    Server 10.0.89.71 (has 33.33% masters): (masters: 2   slaves: 4  )
    Server 10.0.89.72 (has 33.33% masters): (masters: 2   slaves: 4  )
    Server 10.0.89.76 (has 33.33% masters): (masters: 2   slaves: 4  )
Skew is 0.0%


And has problems:
    None
Printing new plan:
Attach slave  28d355544294ba91e3c1ce1f3680e5a23506cb2e 10.0.89.71:6773 to 517deae1f5011b7b187b2a51771c0ce68d6e99a0 10.0.89.72:6771
Attach slave  d1ecc8a2ad04d4d22546ba9f4989ea6152157397 10.0.89.71:6774 to 704abb258c6f7981b04641ca9507acb25051f71e 10.0.89.72:6772
Attach slave  c331ed5c1700f8edf8747925c4184bf8ea49d918 10.0.89.72:6774 to e1b8bdf9db261ec5cd660d0463f9352647497770 10.0.89.76:6771
Attach slave  2c15b9b3ed26469129ccc21d3189aa3fbc35cc26 10.0.89.72:6773 to 080dddce00b1e0a8031e93b98a87a476ab02c6eb 10.0.89.76:6772
Attach slave  e6001cc74de9dae69c2a862984692af0ebc19609 10.0.89.76:6774 to e0973edf8af88dda988f6a457792c748203ecd91 10.0.89.71:6771
Attach slave  22f8733496dfae5979fe375781cfd3eb897c9098 10.0.89.76:6773 to 5870cfef30a39b92471ba4dfd035966851aa5ecc 10.0.89.71:6772

It will take 6 iterations with timeout 90 and will take 0:09:00 time

Cluster will have instances per group:
    Server 10.0.89.71 (has 33.33% masters): (masters: 2   slaves: 2  )
    Server 10.0.89.72 (has 33.33% masters): (masters: 2   slaves: 2  )
    Server 10.0.89.76 (has 33.33% masters): (masters: 2   slaves: 2  )
Skew is 0.0%


And will have problems
    None

Proceed plan to execute with timeout 90 seconds between operations? y/n
```
# Datacenter use
If you have cross datacenter cluster, you must write your own inventory class (make requests in it, parse yamls, do whatever you want) and define inventory_helper in main section of code.
Redisclustertool perfectly spread all redis nodes across datacenters and level out masters in each datacenter

## Example
Get the worst example for demonstration

```
[root@server1 ~]$ redisclustertool.py -p 7000
Processing with replica count 2 and use port 7000
Now cluster has instances per group:
    Group DC1 (has 33.33% masters): (masters: 24  slaves: 48 )
        host redis-server1.domain (10.0.104.5) has 100.0% masters of datacenter: (masters: 24 slaves: 24 )
        host redis-server2.domain (10.0.92.12) has 0.0% masters of datacenter: (masters: 0  slaves: 24 )
    Group DC2 (has 0.0% masters): (masters: 0   slaves: 24 ) server redis-server4.domain (10.0.109.7)
    Group DC3 (has 33.33% masters): (masters: 24  slaves: 48 )
        host redis-server3.domain (10.0.19.12) has 0.0% masters of datacenter: (masters: 0  slaves: 24 )
        host redis-server5.domain (10.0.27.11) has 100.0% masters of datacenter: (masters: 24 slaves: 24 )
    Group DC4 (has 33.33% masters): (masters: 24  slaves: 24 ) server redis-server6.domain (10.0.69.5)
Skew: 33.33%
Actual replica count 2


And has problems:
Master-slave distribution problem (15):
    Datacenter DC1 has master 82315892f312141a43c5d718996bbaa972ac5750 10.0.104.5:6729 (redis-server1.domain) with slave f46cd4172e6a4f442ef849b2697dcb7a09c0331b 10.0.27.11:6706 (redis-server5.domain) on same datacenter
    Datacenter DC1 has master 82315892f312141a43c5d718996bbaa972ac5750 10.0.104.5:6729 (redis-server1.domain) with slave 99b761dbe6d3b31fe0f0b7f806edc08e87f65bf9 10.0.92.12:6723 (redis-server2.domain) on same datacenter
    Datacenter DC1 has master 6aac71383ced7170564c60b1a081799b17ee3228 10.0.104.5:6732 (redis-server1.domain) with slave ef38670db365b6c21677e2383e067bf11e5ea78d 10.0.69.5:6708 (redis-server6.domain) on same datacenter
    Datacenter DC1 has master 6aac71383ced7170564c60b1a081799b17ee3228 10.0.104.5:6732 (redis-server1.domain) with slave 79940eb0a7021a33e42477a83629ee87aecc0627 10.0.92.12:6720 (redis-server2.domain) on same datacenter
    Datacenter DC1 has master ed07d9c2d1d5fe1c186884e6ede372241556ba4d 10.0.104.5:6736 (redis-server1.domain) with slave 42c3a22ad96db2e8414b928ed11996848092374d 10.0.69.5:6712 (redis-server6.domain) on same datacenter
    Datacenter DC1 has master ed07d9c2d1d5fe1c186884e6ede372241556ba4d 10.0.104.5:6736 (redis-server1.domain) with slave ef7ed5eba999b81487f1ab3e7f784ee56b0ecb63 10.0.92.12:6711 (redis-server2.domain) on same datacenter
    Datacenter DC1 has master a4bcca9e8d25c1801c8fb83def75daa44174308e 10.0.104.5:6742 (redis-server1.domain) with slave f29bc950aeb4d992b534be0e3180c16c00a7f626 10.0.69.5:6718 (redis-server6.domain) on same datacenter
    Datacenter DC1 has master a4bcca9e8d25c1801c8fb83def75daa44174308e 10.0.104.5:6742 (redis-server1.domain) with slave 2d5e5ccb7c7c30c63270219809956f0cb32a3511 10.0.92.12:6715 (redis-server2.domain) on same datacenter
    Datacenter DC1 has master 168f8e01e1b508c4cd56398c75836743fad9feec 10.0.104.5:6745 (redis-server1.domain) with slave 71174cb4bc72f4007f2f76f86c4107eff6357613 10.0.27.11:6722 (redis-server5.domain) on same datacenter
    Datacenter DC1 has master 168f8e01e1b508c4cd56398c75836743fad9feec 10.0.104.5:6745 (redis-server1.domain) with slave 0a917e7a1dea5ab95e1f9ae44e63ef84e0152133 10.0.92.12:6707 (redis-server2.domain) on same datacenter
    Datacenter DC1 has master bcfbe97b779e43bbf5c90d30b29e9e9edb81bbe0 10.0.104.5:6746 (redis-server1.domain) with slave 61bae11b1a7a263c2f5fae14ef4acbcdd6862a96 10.0.69.5:6722 (redis-server6.domain) on same datacenter
    Datacenter DC1 has master bcfbe97b779e43bbf5c90d30b29e9e9edb81bbe0 10.0.104.5:6746 (redis-server1.domain) with slave ae5d92cf9455cd211f355b0cde500d9e4f0f36bd 10.0.92.12:6724 (redis-server2.domain) on same datacenter
    Datacenter DC3 has master 44801eb0db9669255e690a6a75ce32610f82b17a 10.0.27.11:6725 (redis-server5.domain) with slave e2a5699a3d7e4117b57a1c4c61a6851f871721a5 10.0.19.12:6713 (redis-server3.domain) on same datacenter
    Datacenter DC3 has master 44801eb0db9669255e690a6a75ce32610f82b17a 10.0.27.11:6725 (redis-server5.domain) with slave 7d09b92e08793c68f957d10f17a76698b27e840f 10.0.69.5:6701 (redis-server6.domain) on same datacenter
    Datacenter DC3 has master 6e2a854e98bbaec15bd37514a53c629c49ad5f4f 10.0.27.11:6726 (redis-server5.domain) with slave 91a6a8668b1c4f819a6c9aa4de32a6f57108c621 10.0.104.5:6701 (redis-server1.domain) on same datacenter
    Datacenter DC3 has master 6e2a854e98bbaec15bd37514a53c629c49ad5f4f 10.0.27.11:6726 (redis-server5.domain) with slave 5e075202d756c821cd98d6f6c4bd282fb947038d 10.0.19.12:6718 (redis-server3.domain) on same datacenter
    Datacenter DC3 has master c115e18c42f5a0efdaf8d199fef5452f1bdb7262 10.0.27.11:6729 (redis-server5.domain) with slave e4ff54bb25e18dc922b51ca171ff25ff344422b2 10.0.19.12:6723 (redis-server3.domain) on same datacenter
    Datacenter DC3 has master c115e18c42f5a0efdaf8d199fef5452f1bdb7262 10.0.27.11:6729 (redis-server5.domain) with slave 2df02cf6b81f5daa1344263116fbc07aeb4e9920 10.0.69.5:6705 (redis-server6.domain) on same datacenter
    Datacenter DC3 has master 978b93268fdc6cc1f1a52e6630e14ea28d624c88 10.0.27.11:6731 (redis-server5.domain) with slave 4bfaa8d2b0ab87679fb21b4a0026d00e01a5010d 10.0.19.12:6722 (redis-server3.domain) on same datacenter
    Datacenter DC3 has master 978b93268fdc6cc1f1a52e6630e14ea28d624c88 10.0.27.11:6731 (redis-server5.domain) with slave 52b164ae7cda76c7774df056305a592cd3326210 10.0.69.5:6707 (redis-server6.domain) on same datacenter
    Datacenter DC3 has master dd8208825fff497b93d1a6cdbcef0b3459c7da63 10.0.27.11:6732 (redis-server5.domain) with slave ad57d0ba0244ba02c47654e80c4d7eab903bea67 10.0.104.5:6707 (redis-server1.domain) on same datacenter
    Datacenter DC3 has master dd8208825fff497b93d1a6cdbcef0b3459c7da63 10.0.27.11:6732 (redis-server5.domain) with slave 12ab7d872cd432e42475b8b8f87baa0d2d366163 10.0.19.12:6710 (redis-server3.domain) on same datacenter
    Datacenter DC3 has master a8b96db283c586f543c60823e7608d8fde5bf300 10.0.27.11:6738 (redis-server5.domain) with slave a8128d09c67ea4566112d3603a47f5066a73583d 10.0.104.5:6713 (redis-server1.domain) on same datacenter
    Datacenter DC3 has master a8b96db283c586f543c60823e7608d8fde5bf300 10.0.27.11:6738 (redis-server5.domain) with slave 5ce7e64c61152bb03672d0a0d16405edc8c8b876 10.0.19.12:6715 (redis-server3.domain) on same datacenter
    Datacenter DC3 has master 97f7ae799eaeada22652a4ca8faefbbf67f17016 10.0.27.11:6742 (redis-server5.domain) with slave 5f6a0bcce187dee8dc63f202eec2ade63327c2ca 10.0.104.5:6717 (redis-server1.domain) on same datacenter
    Datacenter DC3 has master 97f7ae799eaeada22652a4ca8faefbbf67f17016 10.0.27.11:6742 (redis-server5.domain) with slave ae5d1ebd3f15f447c6bb41f5748e634112579665 10.0.19.12:6709 (redis-server3.domain) on same datacenter
    Datacenter DC3 has master 251d9183e012b488bf09d3df33bc36ff02d63c7a 10.0.27.11:6746 (redis-server5.domain) with slave 2b4a812a3cfc4b0a2ea24cd88a9424443e8d5c42 10.0.104.5:6721 (youla-mem3-1.p) on same datacenter
    Datacenter DC3 has master 251d9183e012b488bf09d3df33bc36ff02d63c7a 10.0.27.11:6746 (redis-server5.domain) with slave 2a4e4d535d54bb5186c14b063f7de36a1a6f314e 10.0.19.12:6701 (redis-server3.domain) on same datacenter
    Datacenter DC3 has master b3f3eba9f6bd28ad29a02408f4f3f213770ca89d 10.0.27.11:6747 (redis-server5.domain) with slave 002921a875f6e4abfe0e50d52a9b248322c48cbe 10.0.19.12:6708 (redis-server3.domain) on same datacenter
    Datacenter DC3 has master b3f3eba9f6bd28ad29a02408f4f3f213770ca89d 10.0.27.11:6747 (redis-server5.domain) with slave b401b09182e5fc8b9800848b1a2ee9279c48405a 10.0.69.5:6723 (redis-server6.domain) on same datacenter

Too many slaves of one master in group problems (16):
    Datacenter DC3 has master 128b2ccbfa75b56f2cfd5ddda05cec17ce230bf3 10.0.27.11:6728 (redis-server5.domain) with 2 slaves c46662c3905de03c49e199cfa9d407c565e05aea 10.0.104.5:6703 and a7a2e53224a52a96f51cdaa107e295629f9dc562 10.0.92.12:6704 placed in one datacenter DC1
    Datacenter DC4 has master 13e7a921cd3d37abd540826751f94d70aa95b3f5 10.0.69.5:6728 (redis-server6.domain) with 2 slaves 5f739dfb066ddf754cc4aab10ffa93b6bf2b22fa 10.0.104.5:6704 and 19f7db4170540e2ac5e96d8d58e5fcbb2c278276 10.0.92.12:6722 placed in one datacenter DC1
    Datacenter DC4 has master 1485928dc5e43600dee7629527b99b1665e70ecb 10.0.69.5:6738 (redis-server6.domain) with 2 slaves 520b546523ee9a97b8be5a971fdbb58eaeb11154 10.0.104.5:6714 and 87fb689c7c828ed24cf7e7f16f0e0f0bb21aab0e 10.0.92.12:6717 placed in one datacenter DC1
    Datacenter DC4 has master 287eacab0133afe7143a0309b9fe5551f43766ae 10.0.69.5:6736 (redis-server6.domain) with 2 slaves 1fad4fd420e08a7134969f5e033aa5c01e5259e2 10.0.104.5:6712 and 1bedc54e65a97264b66039ae56cd3359b287c69f 10.0.92.12:6709 placed in one datacenter DC1
    Datacenter DC3 has master 4cf3be1eda3714268403ac98dd034f407eb273a0 10.0.27.11:6736 (redis-server5.domain) with 2 slaves 3b140f078309766e839f57e62cb51301115d0063 10.0.104.5:6711 and a11b758531f40330e5183fb2c87139869563f504 10.0.92.12:6714 placed in one datacenter DC1
    Datacenter DC3 has master 556e6f740dce844c8270e357d6e5460e2523ec38 10.0.27.11:6748 (redis-server5.domain) with 2 slaves 2612d1918204f5dc8d918c4f54c3a04741b08a11 10.0.104.5:6723 and 4998a6fec7cfc06d405dd09a39d4e3953c507c11 10.0.92.12:6712 placed in one datacenter DC1
    Datacenter DC4 has master 7ce1bd48833f2c0403845f92faf641ddd4fe2a15 10.0.69.5:6730 (redis-server6.domain) with 2 slaves c110b893ff46dc294b3efb1c386f7863d032b409 10.0.104.5:6706 and 8e3655217c5543aa6ed846a5e80e5d227bd2c1a9 10.0.92.12:6701 placed in one datacenter DC1
    Datacenter DC3 has master 8381e62ff9fb1177b881935ac8b1eb43a17298f7 10.0.27.11:6734 (redis-server5.domain) with 2 slaves fe2f602a8c8e5fba3c50ae16b3923cdf3c03470a 10.0.104.5:6709 and ead08a680ab686246a347ca7b7e4210004d05c5e 10.0.92.12:6716 placed in one datacenter DC1
    Datacenter DC3 has master e86fa7c409057b1193ef6d51b8829a7935bfd32b 10.0.27.11:6730 (redis-server5.domain) with 2 slaves 38c859b04fff7b35f0e4c9135431bf86b0889d07 10.0.104.5:6705 and e170ac27145a98ae1fccf949e386f7cf4c143f7c 10.0.92.12:6710 placed in one datacenter DC1
    Datacenter DC1 has master 0c4dba285011e8819620ae14eecf859e424fa02b 10.0.104.5:6731 (redis-server1.domain) with 2 slaves 95f4f5476f02d491a3c26e7bb882bdf1765cd26d 10.0.19.12:6712 and 694987420a6382a396211fee898ea26d2169213d 10.0.27.11:6708 placed in one datacenter DC3
    Datacenter DC4 has master 17315b0c2a4311665e1c9b13bcc2ba8956a1954c 10.0.69.5:6737 (redis-server6.domain) with 2 slaves cf2d21379bcdcd8931cb720f500e51e4eebb6710 10.0.19.12:6703 and 254dee8c8eae865eaef564d7bb2474297470c97a 10.0.27.11:6713 placed in one datacenter DC3
    Datacenter DC1 has master 2bb9ebc403ec41fa128a9cc079d4a3b5184b302f 10.0.104.5:6741 (redis-server1.domain) with 2 slaves e3dc260981e8a10f8cf4aa9ce3b0514f1a7194fd 10.0.19.12:6704 and 724dc2fc67ce4ddd4c4a621026b9a6a8d87a223e 10.0.27.11:6718 placed in one datacenter DC3
    Datacenter DC4 has master 8d67a4e4174754cf14d83c74c1c1c2b7ab4f555d 10.0.69.5:6747 (redis-server6.domain) with 2 slaves d5f3fca8f7bf6a9d5f35153dc63f4470c6758bcf 10.0.19.12:6724 and d1fcac786e5708dad568964b713c2a1ee28552b9 10.0.27.11:6723 placed in one datacenter DC3
    Datacenter DC1 has master 94252dca0d5358fc329a79c47426629049ec02c8 10.0.104.5:6735 (redis-server1.domain) with 2 slaves 67790e85d4cbc6a7289892c21698ad0d658ba5f7 10.0.19.12:6707 and dab616e9af00932c47201539286a6479706c3041 10.0.27.11:6712 placed in one datacenter DC3
    Datacenter DC4 has master a5d6b9cda10b887f93818f6adca399e72d8cdd96 10.0.69.5:6743 (redis-server6.domain) with 2 slaves 35c77b7b1feb705c49f672cb8017fe05ee085a5e 10.0.19.12:6721 and d7333b37e2ea241a61a4a9a5e074b845a32af691 10.0.27.11:6719 placed in one datacenter DC3
    Datacenter DC1 has master ffd80a71cc38fa6d71835aaee012ea6be15bd4b2 10.0.104.5:6727 (redis-server1.domain) with 2 slaves ab4c8af95bf965d2ee56690c51e7a86eae48ced2 10.0.19.12:6714 and f9db74b5c63c644d3b14a824991e7ec4dd6d948d 10.0.27.11:6704 placed in one datacenter DC3

Groups have master distribution skew more than 15% (actual 33.33%): {'DC1': 33.33, 'DC2': 0.0, 'DC3': 33.33, 'DC4': 33.33}

Group DC1 has servers with distribution skew more than 30% in group (actual 100.0%): {'10.0.104.5': 100.0, '10.0.92.12': 0.0}

Group DC3 has servers with distribution skew more than 30% in group (actual 100.0%): {'10.0.27.11': 100.0, '10.0.19.12': 0.0}

Printing new plan:
Failover node 48c47ec1ed34df3936a266229f262670d9d908bc 10.0.109.7:6701 group DC2 [old master 0898de69767861d7056886a4196f10116149dbdf 10.0.104.5:6737 group DC1]
Failover node 7ab717f988cf6ffe7ae76ab3a21ddd8a1f91e621 10.0.19.12:6702 group DC3 [old master e5e8956b06e505718967ede470fb4ed860a106db 10.0.104.5:6730 group DC1]
Failover node d616aa43441d28a4c8795325fc720f1a3b9fb6e5 10.0.109.7:6705 group DC2 [old master 483b5e47e11c4c4805494af2f43f395965c435e7 10.0.104.5:6743 group DC1]
Failover node e3dc260981e8a10f8cf4aa9ce3b0514f1a7194fd 10.0.19.12:6704 group DC3 [old master 2bb9ebc403ec41fa128a9cc079d4a3b5184b302f 10.0.104.5:6741 group DC1]
Failover node 2b033cb892ffb7e14d611486193b3e1634b7e85b 10.0.109.7:6706 group DC2 [old master 5c181828ae06fe48981369065d6fcc97c38a90ed 10.0.104.5:6725 group DC1]
Failover node 67790e85d4cbc6a7289892c21698ad0d658ba5f7 10.0.19.12:6707 group DC3 [old master 94252dca0d5358fc329a79c47426629049ec02c8 10.0.104.5:6735 group DC1]
Failover node 64ddc03fdef22a90c5d0a97fc99321ad91ac03c2 10.0.109.7:6708 group DC2 [old master 66c10833f39babebbd83984662544abe36b71499 10.0.104.5:6747 group DC1]
Failover node 95f4f5476f02d491a3c26e7bb882bdf1765cd26d 10.0.19.12:6712 group DC3 [old master 0c4dba285011e8819620ae14eecf859e424fa02b 10.0.104.5:6731 group DC1]
Failover node 37b0b4845fa45e5ac3cd31ff39cadab8eaa947a4 10.0.109.7:6709 group DC2 [old master d8432555042361ec2b19082e378064a5512a339f 10.0.104.5:6739 group DC1]
Failover node ab4c8af95bf965d2ee56690c51e7a86eae48ced2 10.0.19.12:6714 group DC3 [old master ffd80a71cc38fa6d71835aaee012ea6be15bd4b2 10.0.104.5:6727 group DC1]
Failover node 891f73ab4fcf90b09e7bd0258ddd2cf23d7b6cc9 10.0.109.7:6714 group DC2 [old master f8b922684a9cbeb05b4e537a9112b112196b655f 10.0.104.5:6734 group DC1]
Failover node a247e7f10b38e8271db3b8140f588e81202a8aee 10.0.19.12:6717 group DC3 [old master 3e8913a48e99714c1fed19012d5915591e6dde06 10.0.104.5:6738 group DC1]
Failover node 8da3b95602d58a1f0ecc65d4aa9ca3e087a44328 10.0.109.7:6716 group DC2 [old master 0141fe1ee52fc9f410bdc9c059d66ffbb5dac22d 10.0.104.5:6748 group DC1]
Failover node 5f607dffb08662697e880ae34d6758cebf06d50c 10.0.109.7:6718 group DC2 [old master 2a419f273f90c254b83a9f588b4f57312136093d 10.0.104.5:6740 group DC1]
Failover node 96dd41b945f4f9e5351076f652a34fe1f36f3f4f 10.0.109.7:6719 group DC2 [old master a17d1d545ed92c2f35cf487bb2c883972a6c7b52 10.0.104.5:6726 group DC1]
Failover node 928bf76515c2a8ef3d68fdfd2f4ec31bb3eae2db 10.0.109.7:6721 group DC2 [old master b0381fb3eb29d25710b726806a59e5d719275d74 10.0.104.5:6728 group DC1]
Failover node 0c98489ee5665355dcdeaebf0e3f7620bd52e3b3 10.0.109.7:6722 group DC2 [old master 96ba412fc2be16c070406e048c200da207955f8f 10.0.104.5:6744 group DC1]
Failover node 864eca21c4ca364bbddf26ac117d3d8f641111ec 10.0.109.7:6724 group DC2 [old master f2d42060ba1ad19707e1d736767a73fd4d13379e 10.0.104.5:6733 group DC1]
Attach slave  8defb187bfd742faeaf68cb48b4c7a009f58a54e 10.0.109.7:6702 group DC2 to bcfbe97b779e43bbf5c90d30b29e9e9edb81bbe0 10.0.104.5:6746 group DC1
Failover node 8defb187bfd742faeaf68cb48b4c7a009f58a54e 10.0.109.7:6702 group DC2 [old master bcfbe97b779e43bbf5c90d30b29e9e9edb81bbe0 10.0.104.5:6746 group DC1]
Attach slave  8e3655217c5543aa6ed846a5e80e5d227bd2c1a9 10.0.92.12:6701 group DC1 to 168f8e01e1b508c4cd56398c75836743fad9feec 10.0.104.5:6745 group DC1
Failover node 8e3655217c5543aa6ed846a5e80e5d227bd2c1a9 10.0.92.12:6701 group DC1 [old master 168f8e01e1b508c4cd56398c75836743fad9feec 10.0.104.5:6745 group DC1]
Attach slave  283e7487df9e19465ba95f326f717fbb3b9eebb0 10.0.109.7:6703 group DC2 to a4bcca9e8d25c1801c8fb83def75daa44174308e 10.0.104.5:6742 group DC1
Failover node 283e7487df9e19465ba95f326f717fbb3b9eebb0 10.0.109.7:6703 group DC2 [old master a4bcca9e8d25c1801c8fb83def75daa44174308e 10.0.104.5:6742 group DC1]
Attach slave  5ca3807d8c2e92a43a2523e46d0a2fc69218d58e 10.0.92.12:6702 group DC1 to ed07d9c2d1d5fe1c186884e6ede372241556ba4d 10.0.104.5:6736 group DC1
Failover node 5ca3807d8c2e92a43a2523e46d0a2fc69218d58e 10.0.92.12:6702 group DC1 [old master ed07d9c2d1d5fe1c186884e6ede372241556ba4d 10.0.104.5:6736 group DC1]
Attach slave  83bcea30c7407bc88f1a2a2c5cd143554ca5882e 10.0.109.7:6704 group DC2 to 6aac71383ced7170564c60b1a081799b17ee3228 10.0.104.5:6732 group DC1
Failover node 83bcea30c7407bc88f1a2a2c5cd143554ca5882e 10.0.109.7:6704 group DC2 [old master 6aac71383ced7170564c60b1a081799b17ee3228 10.0.104.5:6732 group DC1]
Attach slave  b8e558b83a0c084ae933fb766961186652e90a18 10.0.92.12:6703 group DC1 to 82315892f312141a43c5d718996bbaa972ac5750 10.0.104.5:6729 group DC1
Failover node b8e558b83a0c084ae933fb766961186652e90a18 10.0.92.12:6703 group DC1 [old master 82315892f312141a43c5d718996bbaa972ac5750 10.0.104.5:6729 group DC1]
Attach slave  a7a2e53224a52a96f51cdaa107e295629f9dc562 10.0.92.12:6704 group DC1 to 556e6f740dce844c8270e357d6e5460e2523ec38 10.0.27.11:6748 group DC3
Failover node a7a2e53224a52a96f51cdaa107e295629f9dc562 10.0.92.12:6704 group DC1 [old master 556e6f740dce844c8270e357d6e5460e2523ec38 10.0.27.11:6748 group DC3]
Attach slave  b2323e0d57bdba215f6babbc3b28f12c2075f600 10.0.92.12:6705 group DC1 to b3f3eba9f6bd28ad29a02408f4f3f213770ca89d 10.0.27.11:6747 group DC3
Failover node b2323e0d57bdba215f6babbc3b28f12c2075f600 10.0.92.12:6705 group DC1 [old master b3f3eba9f6bd28ad29a02408f4f3f213770ca89d 10.0.27.11:6747 group DC3]
Attach slave  6c943a6242e4a3a707bce3f0353129651a79f2ec 10.0.92.12:6706 group DC1 to 251d9183e012b488bf09d3df33bc36ff02d63c7a 10.0.27.11:6746 group DC3
Failover node 6c943a6242e4a3a707bce3f0353129651a79f2ec 10.0.92.12:6706 group DC1 [old master 251d9183e012b488bf09d3df33bc36ff02d63c7a 10.0.27.11:6746 group DC3]
Attach slave  0a917e7a1dea5ab95e1f9ae44e63ef84e0152133 10.0.92.12:6707 group DC1 to 90e565fd996539611f4cd8c68ca584ca8d7e6caf 10.0.27.11:6745 group DC3
Failover node 0a917e7a1dea5ab95e1f9ae44e63ef84e0152133 10.0.92.12:6707 group DC1 [old master 90e565fd996539611f4cd8c68ca584ca8d7e6caf 10.0.27.11:6745 group DC3]
Attach slave  1d6101d089e2d005fd5bb16d6d0bfeb98baa01b9 10.0.92.12:6708 group DC1 to fec7679e808a88612a422dc5c4524772dcf82a79 10.0.27.11:6744 group DC3
Failover node 1d6101d089e2d005fd5bb16d6d0bfeb98baa01b9 10.0.92.12:6708 group DC1 [old master fec7679e808a88612a422dc5c4524772dcf82a79 10.0.27.11:6744 group DC3]
Attach slave  1bedc54e65a97264b66039ae56cd3359b287c69f 10.0.92.12:6709 group DC1 to e11d297071767c4b20a4892be2c1d8c42f0cf0b0 10.0.27.11:6743 group DC3
Failover node 1bedc54e65a97264b66039ae56cd3359b287c69f 10.0.92.12:6709 group DC1 [old master e11d297071767c4b20a4892be2c1d8c42f0cf0b0 10.0.27.11:6743 group DC3]
Attach slave  df92f18dc536d49f87474a451bab089d5d767665 10.0.109.7:6707 group DC2 to 97f7ae799eaeada22652a4ca8faefbbf67f17016 10.0.27.11:6742 group DC3
Failover node df92f18dc536d49f87474a451bab089d5d767665 10.0.109.7:6707 group DC2 [old master 97f7ae799eaeada22652a4ca8faefbbf67f17016 10.0.27.11:6742 group DC3]
Attach slave  f1fdf4174149adc3f38b2aebb2bab5abc4e0d6d6 10.0.109.7:6710 group DC2 to 176756624089385e059736c48c063513b329d29f 10.0.27.11:6741 group DC3
Failover node f1fdf4174149adc3f38b2aebb2bab5abc4e0d6d6 10.0.109.7:6710 group DC2 [old master 176756624089385e059736c48c063513b329d29f 10.0.27.11:6741 group DC3]
Attach slave  85b1f44ce14c79f4390b68d02db7b5389c77eb49 10.0.109.7:6711 group DC2 to 6c423a147f696079ee997ccc1dfef0a25737a654 10.0.27.11:6740 group DC3
Failover node 85b1f44ce14c79f4390b68d02db7b5389c77eb49 10.0.109.7:6711 group DC2 [old master 6c423a147f696079ee997ccc1dfef0a25737a654 10.0.27.11:6740 group DC3]
Failover node b14e923bc3c3fe54e669fa0e40c2004416f486fb 10.0.104.5:6702 group DC1 [old master 9b4c35457bbebd45095605ef667aede6b0a26d11 10.0.69.5:6726 group DC4]
Failover node cf2d21379bcdcd8931cb720f500e51e4eebb6710 10.0.19.12:6703 group DC3 [old master 17315b0c2a4311665e1c9b13bcc2ba8956a1954c 10.0.69.5:6737 group DC4]
Failover node 5f739dfb066ddf754cc4aab10ffa93b6bf2b22fa 10.0.104.5:6704 group DC1 [old master 13e7a921cd3d37abd540826751f94d70aa95b3f5 10.0.69.5:6728 group DC4]
Failover node 22b33ab36bac3fdd424b6ec2c4efa86e8ef53db7 10.0.19.12:6706 group DC3 [old master 126e0d49e0c8079856e042ff5a200f7594fa4961 10.0.69.5:6734 group DC4]
Failover node c110b893ff46dc294b3efb1c386f7863d032b409 10.0.104.5:6706 group DC1 [old master 7ce1bd48833f2c0403845f92faf641ddd4fe2a15 10.0.69.5:6730 group DC4]
Failover node e6358a451f11d2ec23e63ae507cfafd3dcbf6f94 10.0.19.12:6711 group DC3 [old master 8126a233b46166d81b20c2b2dfd8e63c109b801a 10.0.69.5:6748 group DC4]
Failover node 6afacae42589604d40774247b98242b6463c0c78 10.0.104.5:6708 group DC1 [old master ebe64b8ff441aa6898ee2c312aec82990bc7e711 10.0.69.5:6732 group DC4]
Failover node 1fad4fd420e08a7134969f5e033aa5c01e5259e2 10.0.104.5:6712 group DC1 [old master 287eacab0133afe7143a0309b9fe5551f43766ae 10.0.69.5:6736 group DC4]
Failover node 520b546523ee9a97b8be5a971fdbb58eaeb11154 10.0.104.5:6714 group DC1 [old master 1485928dc5e43600dee7629527b99b1665e70ecb 10.0.69.5:6738 group DC4]
Failover node 91a6a8668b1c4f819a6c9aa4de32a6f57108c621 10.0.104.5:6701 group DC1 [old master 6e2a854e98bbaec15bd37514a53c629c49ad5f4f 10.0.27.11:6726 group DC3]
Failover node 7d09b92e08793c68f957d10f17a76698b27e840f 10.0.69.5:6701 group DC4 [old master 44801eb0db9669255e690a6a75ce32610f82b17a 10.0.27.11:6725 group DC3]
Failover node c46662c3905de03c49e199cfa9d407c565e05aea 10.0.104.5:6703 group DC1 [old master 128b2ccbfa75b56f2cfd5ddda05cec17ce230bf3 10.0.27.11:6728 group DC3]
Failover node 1f197bc4b5fd2588b4fcd3662184799a76384c48 10.0.69.5:6703 group DC4 [old master 849aa63f41e38b6b5ff3bc9d527e3bcb2923cdd7 10.0.27.11:6727 group DC3]
Failover node 38c859b04fff7b35f0e4c9135431bf86b0889d07 10.0.104.5:6705 group DC1 [old master e86fa7c409057b1193ef6d51b8829a7935bfd32b 10.0.27.11:6730 group DC3]
Failover node 2df02cf6b81f5daa1344263116fbc07aeb4e9920 10.0.69.5:6705 group DC4 [old master c115e18c42f5a0efdaf8d199fef5452f1bdb7262 10.0.27.11:6729 group DC3]
Attach slave  2a4e4d535d54bb5186c14b063f7de36a1a6f314e 10.0.19.12:6701 group DC3 to 91a6a8668b1c4f819a6c9aa4de32a6f57108c621 10.0.104.5:6701 group DC1
Attach slave  52b164ae7cda76c7774df056305a592cd3326210 10.0.69.5:6707 group DC4 to 91a6a8668b1c4f819a6c9aa4de32a6f57108c621 10.0.104.5:6701 group DC1
Attach slave  002921a875f6e4abfe0e50d52a9b248322c48cbe 10.0.19.12:6708 group DC3 to c46662c3905de03c49e199cfa9d407c565e05aea 10.0.104.5:6703 group DC1
Attach slave  42c3a22ad96db2e8414b928ed11996848092374d 10.0.69.5:6712 group DC4 to c46662c3905de03c49e199cfa9d407c565e05aea 10.0.104.5:6703 group DC1
Attach slave  ae5d1ebd3f15f447c6bb41f5748e634112579665 10.0.19.12:6709 group DC3 to 5f739dfb066ddf754cc4aab10ffa93b6bf2b22fa 10.0.104.5:6704 group DC1
Attach slave  0b8d8e6784a4732b41279fe8e25026cf7356ebfe 10.0.69.5:6713 group DC4 to 5f739dfb066ddf754cc4aab10ffa93b6bf2b22fa 10.0.104.5:6704 group DC1
Attach slave  12ab7d872cd432e42475b8b8f87baa0d2d366163 10.0.19.12:6710 group DC3 to 38c859b04fff7b35f0e4c9135431bf86b0889d07 10.0.104.5:6705 group DC1
Attach slave  a08a7c45c011ba65a99f8b1b48f7f8e73e3cd81a 10.0.69.5:6715 group DC4 to 38c859b04fff7b35f0e4c9135431bf86b0889d07 10.0.104.5:6705 group DC1
Attach slave  e2a5699a3d7e4117b57a1c4c61a6851f871721a5 10.0.19.12:6713 group DC3 to c110b893ff46dc294b3efb1c386f7863d032b409 10.0.104.5:6706 group DC1
Attach slave  13e7a921cd3d37abd540826751f94d70aa95b3f5 10.0.69.5:6728 group DC4 to c110b893ff46dc294b3efb1c386f7863d032b409 10.0.104.5:6706 group DC1
Attach slave  5ce7e64c61152bb03672d0a0d16405edc8c8b876 10.0.19.12:6715 group DC3 to 1fad4fd420e08a7134969f5e033aa5c01e5259e2 10.0.104.5:6712 group DC1
Attach slave  7ce1bd48833f2c0403845f92faf641ddd4fe2a15 10.0.69.5:6730 group DC4 to 1fad4fd420e08a7134969f5e033aa5c01e5259e2 10.0.104.5:6712 group DC1
Attach slave  5e075202d756c821cd98d6f6c4bd282fb947038d 10.0.19.12:6718 group DC3 to 520b546523ee9a97b8be5a971fdbb58eaeb11154 10.0.104.5:6714 group DC1
Attach slave  287eacab0133afe7143a0309b9fe5551f43766ae 10.0.69.5:6736 group DC4 to 520b546523ee9a97b8be5a971fdbb58eaeb11154 10.0.104.5:6714 group DC1
Attach slave  ad57d0ba0244ba02c47654e80c4d7eab903bea67 10.0.104.5:6707 group DC1 to cf2d21379bcdcd8931cb720f500e51e4eebb6710 10.0.19.12:6703 group DC3
Attach slave  17315b0c2a4311665e1c9b13bcc2ba8956a1954c 10.0.69.5:6737 group DC4 to cf2d21379bcdcd8931cb720f500e51e4eebb6710 10.0.19.12:6703 group DC3
Attach slave  fe2f602a8c8e5fba3c50ae16b3923cdf3c03470a 10.0.104.5:6709 group DC1 to e3dc260981e8a10f8cf4aa9ce3b0514f1a7194fd 10.0.19.12:6704 group DC3
Attach slave  1485928dc5e43600dee7629527b99b1665e70ecb 10.0.69.5:6738 group DC4 to e3dc260981e8a10f8cf4aa9ce3b0514f1a7194fd 10.0.19.12:6704 group DC3
Attach slave  4df20cd0871e4f1bdaf9b2dd50630b0ba1120eb9 10.0.69.5:6717 group DC4 to 67790e85d4cbc6a7289892c21698ad0d658ba5f7 10.0.19.12:6707 group DC3
Attach slave  3b140f078309766e839f57e62cb51301115d0063 10.0.104.5:6711 group DC1 to f1fdf4174149adc3f38b2aebb2bab5abc4e0d6d6 10.0.109.7:6710 group DC2
Attach slave  4df20cd0871e4f1bdaf9b2dd50630b0ba1120eb9 10.0.69.5:6717 group DC4 to 67790e85d4cbc6a7289892c21698ad0d658ba5f7 10.0.19.12:6707 group DC3
Attach slave  a8128d09c67ea4566112d3603a47f5066a73583d 10.0.104.5:6713 group DC1 to 67790e85d4cbc6a7289892c21698ad0d658ba5f7 10.0.19.12:6707 group DC3
Attach slave  13ff2e798f49f72adee96ae7e66681ee39b9f01b 10.0.109.7:6720 group DC2 to 95f4f5476f02d491a3c26e7bb882bdf1765cd26d 10.0.19.12:6712 group DC3
Attach slave  adfe94bfc633a919238c39d92e0dccb6fd747200 10.0.104.5:6718 group DC1 to 1f197bc4b5fd2588b4fcd3662184799a76384c48 10.0.69.5:6703 group DC4
Attach slave  13ff2e798f49f72adee96ae7e66681ee39b9f01b 10.0.109.7:6720 group DC2 to 95f4f5476f02d491a3c26e7bb882bdf1765cd26d 10.0.19.12:6712 group DC3
Attach slave  59c5a48f8c1bdd9cc61ee1581e40937c7e06c018 10.0.104.5:6719 group DC1 to 95f4f5476f02d491a3c26e7bb882bdf1765cd26d 10.0.19.12:6712 group DC3
Attach slave  ff061f4916ad4b8e916f3c85ca97c92ee4b5aa26 10.0.109.7:6712 group DC2 to ab4c8af95bf965d2ee56690c51e7a86eae48ced2 10.0.19.12:6714 group DC3
Attach slave  2b4a812a3cfc4b0a2ea24cd88a9424443e8d5c42 10.0.104.5:6721 group DC1 to 8193d5e7650404ebc15b699ffaba341630c77d2f 10.0.69.5:6731 group DC4
Attach slave  ff061f4916ad4b8e916f3c85ca97c92ee4b5aa26 10.0.109.7:6712 group DC2 to ab4c8af95bf965d2ee56690c51e7a86eae48ced2 10.0.19.12:6714 group DC3
Attach slave  2612d1918204f5dc8d918c4f54c3a04741b08a11 10.0.104.5:6723 group DC1 to ab4c8af95bf965d2ee56690c51e7a86eae48ced2 10.0.19.12:6714 group DC3
Attach slave  cb7b1b88ebff1bf19205b9edb79d6575c06ad2ef 10.0.109.7:6715 group DC2 to 978b93268fdc6cc1f1a52e6630e14ea28d624c88 10.0.27.11:6731 group DC3
Attach slave  ffd80a71cc38fa6d71835aaee012ea6be15bd4b2 10.0.104.5:6727 group DC1 to 91e22d36e977c5799474a8d2a6d279858d1ecbb2 10.0.69.5:6741 group DC4
Attach slave  cb7b1b88ebff1bf19205b9edb79d6575c06ad2ef 10.0.109.7:6715 group DC2 to 978b93268fdc6cc1f1a52e6630e14ea28d624c88 10.0.27.11:6731 group DC3
Attach slave  82315892f312141a43c5d718996bbaa972ac5750 10.0.104.5:6729 group DC1 to 978b93268fdc6cc1f1a52e6630e14ea28d624c88 10.0.27.11:6731 group DC3
Attach slave  0c4dba285011e8819620ae14eecf859e424fa02b 10.0.104.5:6731 group DC1 to dd8208825fff497b93d1a6cdbcef0b3459c7da63 10.0.27.11:6732 group DC3
Attach slave  9b4c35457bbebd45095605ef667aede6b0a26d11 10.0.69.5:6726 group DC4 to dd8208825fff497b93d1a6cdbcef0b3459c7da63 10.0.27.11:6732 group DC3
Attach slave  6aac71383ced7170564c60b1a081799b17ee3228 10.0.104.5:6732 group DC1 to 8381e62ff9fb1177b881935ac8b1eb43a17298f7 10.0.27.11:6734 group DC3
Attach slave  ebe64b8ff441aa6898ee2c312aec82990bc7e711 10.0.69.5:6732 group DC4 to 8381e62ff9fb1177b881935ac8b1eb43a17298f7 10.0.27.11:6734 group DC3
Attach slave  94252dca0d5358fc329a79c47426629049ec02c8 10.0.104.5:6735 group DC1 to 4cf3be1eda3714268403ac98dd034f407eb273a0 10.0.27.11:6736 group DC3
Attach slave  61bae11b1a7a263c2f5fae14ef4acbcdd6862a96 10.0.69.5:6722 group DC4 to 4cf3be1eda3714268403ac98dd034f407eb273a0 10.0.27.11:6736 group DC3
Attach slave  ed07d9c2d1d5fe1c186884e6ede372241556ba4d 10.0.104.5:6736 group DC1 to 83aaacb9b577b136b0542e455b7f5a13d42eb8a9 10.0.27.11:6737 group DC3
Attach slave  f29bc950aeb4d992b534be0e3180c16c00a7f626 10.0.69.5:6718 group DC4 to 83aaacb9b577b136b0542e455b7f5a13d42eb8a9 10.0.27.11:6737 group DC3
Attach slave  2bb9ebc403ec41fa128a9cc079d4a3b5184b302f 10.0.104.5:6741 group DC1 to a8b96db283c586f543c60823e7608d8fde5bf300 10.0.27.11:6738 group DC3
Attach slave  ef38670db365b6c21677e2383e067bf11e5ea78d 10.0.69.5:6708 group DC4 to a8b96db283c586f543c60823e7608d8fde5bf300 10.0.27.11:6738 group DC3
Attach slave  a4bcca9e8d25c1801c8fb83def75daa44174308e 10.0.104.5:6742 group DC1 to 5b3bbbd3ccf9ba9c537457c0bfb50fd164b77870 10.0.27.11:6739 group DC3
Attach slave  8c45a2a77732cfe640608156f4558f5762a63dd5 10.0.69.5:6710 group DC4 to 5b3bbbd3ccf9ba9c537457c0bfb50fd164b77870 10.0.27.11:6739 group DC3
Attach slave  168f8e01e1b508c4cd56398c75836743fad9feec 10.0.104.5:6745 group DC1 to 7d09b92e08793c68f957d10f17a76698b27e840f 10.0.69.5:6701 group DC4
Attach slave  35c77b7b1feb705c49f672cb8017fe05ee085a5e 10.0.19.12:6721 group DC3 to 7d09b92e08793c68f957d10f17a76698b27e840f 10.0.69.5:6701 group DC4
Attach slave  bcfbe97b779e43bbf5c90d30b29e9e9edb81bbe0 10.0.104.5:6746 group DC1 to 2df02cf6b81f5daa1344263116fbc07aeb4e9920 10.0.69.5:6705 group DC4
Attach slave  4bfaa8d2b0ab87679fb21b4a0026d00e01a5010d 10.0.19.12:6722 group DC3 to 2df02cf6b81f5daa1344263116fbc07aeb4e9920 10.0.69.5:6705 group DC4
Attach slave  e4ff54bb25e18dc922b51ca171ff25ff344422b2 10.0.19.12:6723 group DC3 to 9479d825a593b61dfd3c1a7dbdcb36aea6d05e54 10.0.69.5:6727 group DC4
Attach slave  e170ac27145a98ae1fccf949e386f7cf4c143f7c 10.0.92.12:6710 group DC1 to 9479d825a593b61dfd3c1a7dbdcb36aea6d05e54 10.0.69.5:6727 group DC4
Attach slave  d5f3fca8f7bf6a9d5f35153dc63f4470c6758bcf 10.0.19.12:6724 group DC3 to ff6b8b5f25c46911cbaf7098d1f847f06d6cbc2a 10.0.69.5:6729 group DC4
Attach slave  ef7ed5eba999b81487f1ab3e7f784ee56b0ecb63 10.0.92.12:6711 group DC1 to ff6b8b5f25c46911cbaf7098d1f847f06d6cbc2a 10.0.69.5:6729 group DC4
Attach slave  ca0c0d85a15cd0245eb59dc7ae31b2abc6ff3b8c 10.0.27.11:6703 group DC3 to b7784e81104ea18eca8ce797f21d6c7c7acc70b0 10.0.69.5:6733 group DC4
Attach slave  4998a6fec7cfc06d405dd09a39d4e3953c507c11 10.0.92.12:6712 group DC1 to b7784e81104ea18eca8ce797f21d6c7c7acc70b0 10.0.69.5:6733 group DC4
Attach slave  f9db74b5c63c644d3b14a824991e7ec4dd6d948d 10.0.27.11:6704 group DC3 to 8424976d1b2ecc5303a226cec91f77742696010a 10.0.69.5:6735 group DC4
Attach slave  a11b758531f40330e5183fb2c87139869563f504 10.0.92.12:6714 group DC1 to 8424976d1b2ecc5303a226cec91f77742696010a 10.0.69.5:6735 group DC4
Attach slave  c2cac5ef0a13e8f9174af6e9fb8875b7761bafb4 10.0.27.11:6705 group DC3 to f244efbe678fb5dbc992a4e9058b6ac57a21eb2c 10.0.69.5:6742 group DC4
Attach slave  ead08a680ab686246a347ca7b7e4210004d05c5e 10.0.92.12:6716 group DC1 to f244efbe678fb5dbc992a4e9058b6ac57a21eb2c 10.0.69.5:6742 group DC4
Attach slave  f46cd4172e6a4f442ef849b2697dcb7a09c0331b 10.0.27.11:6706 group DC3 to a5d6b9cda10b887f93818f6adca399e72d8cdd96 10.0.69.5:6743 group DC4
Attach slave  87fb689c7c828ed24cf7e7f16f0e0f0bb21aab0e 10.0.92.12:6717 group DC1 to a5d6b9cda10b887f93818f6adca399e72d8cdd96 10.0.69.5:6743 group DC4
Attach slave  694987420a6382a396211fee898ea26d2169213d 10.0.27.11:6708 group DC3 to 8d67a4e4174754cf14d83c74c1c1c2b7ab4f555d 10.0.69.5:6747 group DC4
Attach slave  19f7db4170540e2ac5e96d8d58e5fcbb2c278276 10.0.92.12:6722 group DC1 to 8d67a4e4174754cf14d83c74c1c1c2b7ab4f555d 10.0.69.5:6747 group DC4
Attach slave  4847a04c863a91f184b0cddb6d8abe15bde0266e 10.0.69.5:6724 group DC4 to 8e3655217c5543aa6ed846a5e80e5d227bd2c1a9 10.0.92.12:6701 group DC1
Attach slave  4fa812fa32198a8afc5e39924b2e78d8c36eedbe 10.0.27.11:6709 group DC3 to 8da3b95602d58a1f0ecc65d4aa9ca3e087a44328 10.0.109.7:6716 group DC2
Attach slave  4847a04c863a91f184b0cddb6d8abe15bde0266e 10.0.69.5:6724 group DC4 to 8e3655217c5543aa6ed846a5e80e5d227bd2c1a9 10.0.92.12:6701 group DC1
Attach slave  79c8734cb3824e417b4c26ef6ce3ba39e9eeffcf 10.0.27.11:6711 group DC3 to 8e3655217c5543aa6ed846a5e80e5d227bd2c1a9 10.0.92.12:6701 group DC1
Attach slave  1b90e7a06a474097b481be4410f666e138ea623f 10.0.69.5:6716 group DC4 to 5ca3807d8c2e92a43a2523e46d0a2fc69218d58e 10.0.92.12:6702 group DC1
Attach slave  dab616e9af00932c47201539286a6479706c3041 10.0.27.11:6712 group DC3 to 5f607dffb08662697e880ae34d6758cebf06d50c 10.0.109.7:6718 group DC2
Attach slave  1b90e7a06a474097b481be4410f666e138ea623f 10.0.69.5:6716 group DC4 to 5ca3807d8c2e92a43a2523e46d0a2fc69218d58e 10.0.92.12:6702 group DC1
Attach slave  254dee8c8eae865eaef564d7bb2474297470c97a 10.0.27.11:6713 group DC3 to 5ca3807d8c2e92a43a2523e46d0a2fc69218d58e 10.0.92.12:6702 group DC1
Attach slave  e63b3cad5a596c7c248482b94faa40ba378aa5ea 10.0.69.5:6702 group DC4 to b8e558b83a0c084ae933fb766961186652e90a18 10.0.92.12:6703 group DC1
Attach slave  724dc2fc67ce4ddd4c4a621026b9a6a8d87a223e 10.0.27.11:6718 group DC3 to 96dd41b945f4f9e5351076f652a34fe1f36f3f4f 10.0.109.7:6719 group DC2
Attach slave  e63b3cad5a596c7c248482b94faa40ba378aa5ea 10.0.69.5:6702 group DC4 to b8e558b83a0c084ae933fb766961186652e90a18 10.0.92.12:6703 group DC1
Attach slave  d7333b37e2ea241a61a4a9a5e074b845a32af691 10.0.27.11:6719 group DC3 to b8e558b83a0c084ae933fb766961186652e90a18 10.0.92.12:6703 group DC1
Attach slave  32c513a65e8a4ecb32ebbf4cfcffac901ae05e28 10.0.69.5:6704 group DC4 to a7a2e53224a52a96f51cdaa107e295629f9dc562 10.0.92.12:6704 group DC1
Attach slave  71174cb4bc72f4007f2f76f86c4107eff6357613 10.0.27.11:6722 group DC3 to 928bf76515c2a8ef3d68fdfd2f4ec31bb3eae2db 10.0.109.7:6721 group DC2
Attach slave  32c513a65e8a4ecb32ebbf4cfcffac901ae05e28 10.0.69.5:6704 group DC4 to a7a2e53224a52a96f51cdaa107e295629f9dc562 10.0.92.12:6704 group DC1
Attach slave  d1fcac786e5708dad568964b713c2a1ee28552b9 10.0.27.11:6723 group DC3 to a7a2e53224a52a96f51cdaa107e295629f9dc562 10.0.92.12:6704 group DC1
Attach slave  04afd50173ed6af8ef9aa885f07ce2f08a4587d0 10.0.69.5:6720 group DC4 to 6c943a6242e4a3a707bce3f0353129651a79f2ec 10.0.92.12:6706 group DC1
Attach slave  44801eb0db9669255e690a6a75ce32610f82b17a 10.0.27.11:6725 group DC3 to 0c98489ee5665355dcdeaebf0e3f7620bd52e3b3 10.0.109.7:6722 group DC2
Attach slave  04afd50173ed6af8ef9aa885f07ce2f08a4587d0 10.0.69.5:6720 group DC4 to 6c943a6242e4a3a707bce3f0353129651a79f2ec 10.0.92.12:6706 group DC1
Attach slave  6e2a854e98bbaec15bd37514a53c629c49ad5f4f 10.0.27.11:6726 group DC3 to 6c943a6242e4a3a707bce3f0353129651a79f2ec 10.0.92.12:6706 group DC1
Attach slave  124a01f6f5118bcfa9cf882e3df83276140dc706 10.0.109.7:6723 group DC2 to b14e923bc3c3fe54e669fa0e40c2004416f486fb 10.0.104.5:6702 group DC1
Attach slave  128b2ccbfa75b56f2cfd5ddda05cec17ce230bf3 10.0.27.11:6728 group DC3 to 8f71194500745a2f76876bf36ee2d7baffd49929 10.0.69.5:6744 group DC4
Attach slave  124a01f6f5118bcfa9cf882e3df83276140dc706 10.0.109.7:6723 group DC2 to b14e923bc3c3fe54e669fa0e40c2004416f486fb 10.0.104.5:6702 group DC1
Attach slave  c115e18c42f5a0efdaf8d199fef5452f1bdb7262 10.0.27.11:6729 group DC3 to b14e923bc3c3fe54e669fa0e40c2004416f486fb 10.0.104.5:6702 group DC1
Attach slave  e86fa7c409057b1193ef6d51b8829a7935bfd32b 10.0.27.11:6730 group DC3 to 6afacae42589604d40774247b98242b6463c0c78 10.0.104.5:6708 group DC1
Attach slave  d35ed1578c30d17019bd175050b55ef95df2c168 10.0.69.5:6706 group DC4 to 6afacae42589604d40774247b98242b6463c0c78 10.0.104.5:6708 group DC1
Attach slave  251d9183e012b488bf09d3df33bc36ff02d63c7a 10.0.27.11:6746 group DC3 to 48c47ec1ed34df3936a266229f262670d9d908bc 10.0.109.7:6701 group DC2
Attach slave  99b761dbe6d3b31fe0f0b7f806edc08e87f65bf9 10.0.92.12:6723 group DC1 to 48c47ec1ed34df3936a266229f262670d9d908bc 10.0.109.7:6701 group DC2
Attach slave  556e6f740dce844c8270e357d6e5460e2523ec38 10.0.27.11:6748 group DC3 to 8defb187bfd742faeaf68cb48b4c7a009f58a54e 10.0.109.7:6702 group DC2
Attach slave  0898de69767861d7056886a4196f10116149dbdf 10.0.104.5:6737 group DC1 to 8defb187bfd742faeaf68cb48b4c7a009f58a54e 10.0.109.7:6702 group DC2
Attach slave  e6996c2dcc247c62d74bf8bb2881c11244531af2 10.0.19.12:6705 group DC3 to 283e7487df9e19465ba95f326f717fbb3b9eebb0 10.0.109.7:6703 group DC2
Attach slave  ae5d92cf9455cd211f355b0cde500d9e4f0f36bd 10.0.92.12:6724 group DC1 to 283e7487df9e19465ba95f326f717fbb3b9eebb0 10.0.109.7:6703 group DC2
Attach slave  4ebabec5a418685683c6151f127be1f296bd4af3 10.0.19.12:6716 group DC3 to 83bcea30c7407bc88f1a2a2c5cd143554ca5882e 10.0.109.7:6704 group DC2
Attach slave  2d5e5ccb7c7c30c63270219809956f0cb32a3511 10.0.92.12:6715 group DC1 to 83bcea30c7407bc88f1a2a2c5cd143554ca5882e 10.0.109.7:6704 group DC2
Attach slave  a2a8f1aeabb31ee4814fc2ecf64e4ba2406fa69f 10.0.27.11:6714 group DC3 to d616aa43441d28a4c8795325fc720f1a3b9fb6e5 10.0.109.7:6705 group DC2
Attach slave  79940eb0a7021a33e42477a83629ee87aecc0627 10.0.92.12:6720 group DC1 to d616aa43441d28a4c8795325fc720f1a3b9fb6e5 10.0.109.7:6705 group DC2
Attach slave  483b5e47e11c4c4805494af2f43f395965c435e7 10.0.104.5:6743 group DC1 to 2b033cb892ffb7e14d611486193b3e1634b7e85b 10.0.109.7:6706 group DC2
Attach slave  edcc3a9bcf82e26ee87a736f00ff2c83f68960d6 10.0.27.11:6720 group DC3 to 2b033cb892ffb7e14d611486193b3e1634b7e85b 10.0.109.7:6706 group DC2
Attach slave  5c181828ae06fe48981369065d6fcc97c38a90ed 10.0.104.5:6725 group DC1 to df92f18dc536d49f87474a451bab089d5d767665 10.0.109.7:6707 group DC2
Attach slave  ea0cecc353aab1212d1c3c31eac7bd741e8aa666 10.0.27.11:6702 group DC3 to df92f18dc536d49f87474a451bab089d5d767665 10.0.109.7:6707 group DC2
Attach slave  5f6a0bcce187dee8dc63f202eec2ade63327c2ca 10.0.104.5:6717 group DC1 to 64ddc03fdef22a90c5d0a97fc99321ad91ac03c2 10.0.109.7:6708 group DC2
Attach slave  97f7ae799eaeada22652a4ca8faefbbf67f17016 10.0.27.11:6742 group DC3 to 64ddc03fdef22a90c5d0a97fc99321ad91ac03c2 10.0.109.7:6708 group DC2
Attach slave  66c10833f39babebbd83984662544abe36b71499 10.0.104.5:6747 group DC1 to 37b0b4845fa45e5ac3cd31ff39cadab8eaa947a4 10.0.109.7:6709 group DC2
Attach slave  e6f4e3d7ced35d3cc4e58d464e41e6d7e5647610 10.0.27.11:6724 group DC3 to 37b0b4845fa45e5ac3cd31ff39cadab8eaa947a4 10.0.109.7:6709 group DC2
Attach slave  d8432555042361ec2b19082e378064a5512a339f 10.0.104.5:6739 group DC1 to f1fdf4174149adc3f38b2aebb2bab5abc4e0d6d6 10.0.109.7:6710 group DC2
Attach slave  3b947d59d1ded65db20d0e7ca75b00da4db3a6d8 10.0.27.11:6716 group DC3 to f1fdf4174149adc3f38b2aebb2bab5abc4e0d6d6 10.0.109.7:6710 group DC2
Attach slave  3b140f078309766e839f57e62cb51301115d0063 10.0.104.5:6711 group DC1 to 85b1f44ce14c79f4390b68d02db7b5389c77eb49 10.0.109.7:6711 group DC2
Attach slave  176756624089385e059736c48c063513b329d29f 10.0.27.11:6741 group DC3 to 85b1f44ce14c79f4390b68d02db7b5389c77eb49 10.0.109.7:6711 group DC2
Attach slave  4b50eab757480412f9fd6a90be44f35190c987aa 10.0.104.5:6715 group DC1 to 891f73ab4fcf90b09e7bd0258ddd2cf23d7b6cc9 10.0.109.7:6714 group DC2
Attach slave  6c423a147f696079ee997ccc1dfef0a25737a654 10.0.27.11:6740 group DC3 to 891f73ab4fcf90b09e7bd0258ddd2cf23d7b6cc9 10.0.109.7:6714 group DC2
Attach slave  f8b922684a9cbeb05b4e537a9112b112196b655f 10.0.104.5:6734 group DC1 to 8da3b95602d58a1f0ecc65d4aa9ca3e087a44328 10.0.109.7:6716 group DC2
Attach slave  4fa812fa32198a8afc5e39924b2e78d8c36eedbe 10.0.27.11:6709 group DC3 to 8da3b95602d58a1f0ecc65d4aa9ca3e087a44328 10.0.109.7:6716 group DC2
Attach slave  0141fe1ee52fc9f410bdc9c059d66ffbb5dac22d 10.0.104.5:6748 group DC1 to 5f607dffb08662697e880ae34d6758cebf06d50c 10.0.109.7:6718 group DC2
Attach slave  dab616e9af00932c47201539286a6479706c3041 10.0.27.11:6712 group DC3 to 5f607dffb08662697e880ae34d6758cebf06d50c 10.0.109.7:6718 group DC2
Attach slave  2a419f273f90c254b83a9f588b4f57312136093d 10.0.104.5:6740 group DC1 to 96dd41b945f4f9e5351076f652a34fe1f36f3f4f 10.0.109.7:6719 group DC2
Attach slave  724dc2fc67ce4ddd4c4a621026b9a6a8d87a223e 10.0.27.11:6718 group DC3 to 96dd41b945f4f9e5351076f652a34fe1f36f3f4f 10.0.109.7:6719 group DC2
Attach slave  a17d1d545ed92c2f35cf487bb2c883972a6c7b52 10.0.104.5:6726 group DC1 to 928bf76515c2a8ef3d68fdfd2f4ec31bb3eae2db 10.0.109.7:6721 group DC2
Attach slave  71174cb4bc72f4007f2f76f86c4107eff6357613 10.0.27.11:6722 group DC3 to 928bf76515c2a8ef3d68fdfd2f4ec31bb3eae2db 10.0.109.7:6721 group DC2
Attach slave  b0381fb3eb29d25710b726806a59e5d719275d74 10.0.104.5:6728 group DC1 to 0c98489ee5665355dcdeaebf0e3f7620bd52e3b3 10.0.109.7:6722 group DC2
Attach slave  44801eb0db9669255e690a6a75ce32610f82b17a 10.0.27.11:6725 group DC3 to 0c98489ee5665355dcdeaebf0e3f7620bd52e3b3 10.0.109.7:6722 group DC2
Attach slave  96ba412fc2be16c070406e048c200da207955f8f 10.0.104.5:6744 group DC1 to 864eca21c4ca364bbddf26ac117d3d8f641111ec 10.0.109.7:6724 group DC2
Attach slave  4055a331049bf9f18a18667e1a167045e34faaa5 10.0.27.11:6710 group DC3 to 864eca21c4ca364bbddf26ac117d3d8f641111ec 10.0.109.7:6724 group DC2
Attach slave  f2d42060ba1ad19707e1d736767a73fd4d13379e 10.0.104.5:6733 group DC1 to 7ab717f988cf6ffe7ae76ab3a21ddd8a1f91e621 10.0.19.12:6702 group DC3
Attach slave  126e0d49e0c8079856e042ff5a200f7594fa4961 10.0.69.5:6734 group DC4 to 7ab717f988cf6ffe7ae76ab3a21ddd8a1f91e621 10.0.19.12:6702 group DC3
Attach slave  e5e8956b06e505718967ede470fb4ed860a106db 10.0.104.5:6730 group DC1 to 22b33ab36bac3fdd424b6ec2c4efa86e8ef53db7 10.0.19.12:6706 group DC3
Attach slave  8126a233b46166d81b20c2b2dfd8e63c109b801a 10.0.69.5:6748 group DC4 to 22b33ab36bac3fdd424b6ec2c4efa86e8ef53db7 10.0.19.12:6706 group DC3
Attach slave  b34c9b744832d879027a22dc53839ced6d764242 10.0.104.5:6710 group DC1 to e6358a451f11d2ec23e63ae507cfafd3dcbf6f94 10.0.19.12:6711 group DC3
Attach slave  b6dc12b27f3a6706aef252c8bc1a4e697efabaad 10.0.69.5:6714 group DC4 to e6358a451f11d2ec23e63ae507cfafd3dcbf6f94 10.0.19.12:6711 group DC3
Attach slave  37b93dc10fba62f7f8df8da41d51b784de1503f6 10.0.104.5:6724 group DC1 to a247e7f10b38e8271db3b8140f588e81202a8aee 10.0.19.12:6717 group DC3
Attach slave  1da0fca620b02a63ef808a75c7811400dd8056f3 10.0.109.7:6717 group DC2 to a247e7f10b38e8271db3b8140f588e81202a8aee 10.0.19.12:6717 group DC3
Attach slave  3e8913a48e99714c1fed19012d5915591e6dde06 10.0.104.5:6738 group DC1 to 8651157940034a6e4f4e0cb410876d5a6c739b27 10.0.27.11:6733 group DC3
Attach slave  2220596b8426a66050ecf119392c60cce9fed8de 10.0.69.5:6709 group DC4 to 8651157940034a6e4f4e0cb410876d5a6c739b27 10.0.27.11:6733 group DC3

It will take 199 iterations with timeout 90 and will take 4:58:30 time

Cluster will have instances per group:
    Group DC1 (has 25.0% masters): (masters: 18  slaves: 54 )
        host redis-server1.domain (10.0.104.5) has 50.0% masters of datacenter: (masters: 9  slaves: 39 )
        host redis-server2.domain (10.0.92.12) has 50.0% masters of datacenter: (masters: 9  slaves: 15 )
    Group DC2 (has 25.0% masters): (masters: 18  slaves: 6  ) server redis-server4.domain (10.0.109.7)
    Group DC3 (has 25.0% masters): (masters: 18  slaves: 54 )
        host redis-server3.domain (10.0.19.12) has 50.0% masters of datacenter: (masters: 9  slaves: 15 )
        host redis-server5.domain (10.0.27.11) has 50.0% masters of datacenter: (masters: 9  slaves: 39 )
    Group DC4 (has 25.0% masters): (masters: 18  slaves: 30 ) server redis-server6.domain (10.0.69.5)
Skew: 0.0%
Actual replica count 2


And will have problems
    None

Proceed plan to execute with timeout 90 seconds between operations? y/n

```

# redis-drain

Simple tool for move all masters from server for maintenance

### help
```
./redis-drain.sh                                                                                                                                                                                                                                                              22:08:11

redis-drain.sh: drain all redis masters from 1 host, save them into a file

Usage:
    redis-drain.sh -h <host-where-to-failover-masters.example.com> -p <port> [-s <sleep_time>]

Example:
    redis-drain.sh -h <host> -p <port> -s 45
```
## example


