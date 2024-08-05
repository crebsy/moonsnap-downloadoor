<img src="./assets/moonsnap.png" align="left" width="64" height="64" />
<h1>moonsnap-downloadoor</h1>
<br clear="left"/>

This is a tool for snappy bootstrapping of new full and archive nodes.
It [saves between 66% and 99.2%](#sync-comparison) of the time required to sync a new node from scratch.

## Usage
1. Get a `SNAP_KEY` from https://warpcast.com/crebsy/0xc021a0cf
2. Compile or download the binary from https://dl.moonsnap.xyz/moonsnap
3. Download the snap to your infra with max speed 🚀
4. Start your node and let it sync only a small diff to reach the head of the chain

## Configuration
### Run with pre-built binary
There's a pre-built binary which can be used directly:
```
curl https://dl.moonsnap.xyz/moonsnap -o moonsnap
chmod +x moonsnap
./moonsnap <YOUR_SNAP_KEY> <YOUR_OUT_DIR>
```

### Run with docker
You can also use our pre-built docker image: `ghcr.io/crebsy/moonsnap-downloadoor`
To start, set the following env variables:
```
docker run -e MOONSNAP_SNAP_KEY=<YOUR_SNAP_KEY> -e MOONSNAP_OUT_DIR=<YOUR_OUT_DIR> -d ghcr.io/crebsy/moonsnap-downloadoor
```

### Build locally
You can also build the binary yourself:
```
git clone https://github.com/crebsy/moonsnap-downloadoor.git
cd moonsnap-downloadoor
go build -o moonsnap
./moonsnap <YOUR_SNAP_KEY> <YOUR_OUT_DIR>
```

## Performance
### Sync comparison
| Node             | sync time | with moonsnap | time saving | snap size |
| ---------------- | --------- | ------------- | ----------- | --------- |
| [erigon 3 archive](https://github.com/erigontech/erigon/commit/7931ce92ccbe1be5c16c183962cdbaa916aaeb14) | ~12 hours | ~4 hours      | 66%         | 1.6 TB    |
| [erigon 3 full](https://github.com/erigontech/erigon/commit/12c2732ad92733a6a6aae9db7259062182799674)    | ~36 hours | ~55 minutes   | 97.5%       | 978 GB    |
| [reth 1.0.3 archive](https://github.com/paradigmxyz/reth/releases/tag/v1.0.3) | ~72 hours | ~3 hours     | 96%       | 2.2 TB    |
| [reth 1.0.1 full](https://github.com/paradigmxyz/reth/releases/tag/v1.0.1)   | ~48 hours | ~70 minutes   | 97.5%     | 1.1 TB    |
| lighthouse 5.2.1   | ~17 hours | ~8 minutes    | 99.2%     | 140 GB    |

### Hardware specs
network: 2.5Gbps

disks: RAID0 4x WD Black SN850X NVME 4TB

cpu: AMD Ryzen 9 5900X 12-Core Processor

