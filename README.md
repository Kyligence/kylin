## Technical preview of Kylin 5

> This repo is for developer to read and understand the core part of next gen Kylin's build and query engine.

### Attention
- This repo is a snapshot of enterprise version source code, and will NOT in sync with original repo.
- This repo is core of enterprise version but remove some enterprise level's feature. It includes build engine and query engine at the moment.
- Current this repo NOT open sourced publicly. It is for some approved outside developer to learn and develop their own 
  feature based on this repo.


### Comparison with Kylin 4.0

- Support Table Index
- Support schema change
- Support computed column
- New metadata design remove `CubeInstance`
- New CuboidScheduler
- New Job engine etc.

### Quick Start

1. Build maven artifact with following command:
```shell
mvn clean package -DskipTests -Dcheckstyle.skip=true
```

2. Run unit test with following command:

```shell
mvn clean test -Dcheckstyle.skip=true
```

3. Read [Metadata definition](document/protocol-buffer/metadata.proto)
4. Try to build cuboid/index and run query against very simple ModelDesc in IDEA : [Build and query Index/Cuboid in IDEA](https://github.com/Kyligence/Kylin5/issues/2)