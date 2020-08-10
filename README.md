# MIT 6.824 Spring 2020
https://pdos.csail.mit.edu/6.824/index.html
## Status
- [x] Lab 1 MapReduce
- [x] Lab 2 Raft
    - [x] 2A
    - [x] 2B
    - [x] 2C
- [x] Lab 3 Fault-tolerant Key/Value Service
    - [x] Part A: Key/value service without log compaction
    - [x] Part B: Key/value service with log compaction
- [ ] Lab 4
    - [x] Part A: The Shard Master
    - [ ] Part B: Sharded Key/Value Server

### Lab 1 MapReduce
```
 cd mapreduce/test
 sh ./test-mr.sh
```
Messages like the following can be ignored.
```
2019/12/16 13:27:09 rpc.Register: method "Done" has 1 input parameters; needs exactly three
```
### Lab 2 Raft
- [Raft paper](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)
- [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/)
- [Raft Locking Advice](https://pdos.csail.mit.edu/6.824/labs/raft-locking.txt)
- [Raft Structure Advice](https://pdos.csail.mit.edu/6.824/labs/raft-structure.txt)
```
cd kvdb/internal/raft
go test
```
#### Reference
- 2C: https://www.jianshu.com/p/59a224fded77
### Lab 3 Fault-tolerant Key/Value Service
```
cd kvdb/internal/kvraft
go test
```
#### Reference
- 3B: https://www.jianshu.com/p/8fc46f12a106
### Lab 4 Sharded Key/Value Service
```
cd kvdb/internal/shardmaster
go test
```
#### Reference
- 4B: https://www.jianshu.com/p/f5c8ab9cd577
