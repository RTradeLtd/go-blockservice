# go-blockservice

a fork of `ipfs/go-blockservice` optimized for usage with TemporalX.

# Modifications

* WriteThrough and non-WriteThrough blockservice are condensed into the same one. We leverage the underlying blockstore has logic to avoid excessive writes to disk, and to determine what we need to announce to the network. Only blocks we do not have previously will be announced to the network
* Remove `ipfs/go-log` and use `uber-go/zap` instead

# License

All original code is licensed as it is upstream, modifications are licensed under AGPL-v3 and will be marked accordingly

* [RTrade modifications license](LICENSE.orig)
* [Upstream license](LICENSE.orig)
