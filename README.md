# MIT 6.824 Spring 2020
https://pdos.csail.mit.edu/6.824/index.html
## Status
- [x] Lab 1 MapReduce
- [x] Lab 2 Raft
    - [x] 2A
    - [x] 2B
    - [x] 2C
- [ ] Lab 3 Fault-tolerant Key/Value Service
    - [x] Part A: Key/value service without log compaction
    - [ ] Part B: Key/value service with log compaction
- [ ] Lab 4

### Lab 1 MapReduce
Run map reduce test
```
 cd mapreduce/test
 sh ./test-mr.sh
```
Messages like the following can be ignored.
```
2019/12/16 13:27:09 rpc.Register: method "Done" has 1 input parameters; needs exactly three
```
### Lab 2 Raft
- [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/)
#### Run Test
```
cd raft/pkg/raft
go test -run <2A/2B/2C>
```
### Lab 3 Fault-tolerant Key/Value Service
```
cd raft/pkg/kvdb
go test -run 3A
```
