package blockservice

import (
	"context"
	"testing"

	blockstore "github.com/RTradeLtd/go-ipfs-blockstore/v2"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	butil "github.com/ipfs/go-ipfs-blocksutil"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"go.uber.org/zap/zaptest"
)

func TestBlockservice(t *testing.T) {
	bs := blockstore.NewBlockstore(
		zaptest.NewLogger(t), dssync.MutexWrap(ds.NewMapDatastore()),
	)
	exch := offline.Exchange(bs)
	bserv := New(bs, exch, zaptest.NewLogger(t))
	bgen := butil.NewBlockGenerator()
	for i := 0; i < 2; i++ {
		block := bgen.Next()
		if err := bserv.AddBlock(block); err != nil {
			t.Fatal(err)
		}
		if testBlock, err := bserv.GetBlock(context.Background(), block.Cid()); err != nil {
			t.Fatal(err)
		} else if testBlock.Cid() != block.Cid() {
			t.Fatal("bad block returned")
		}
		if err := bserv.AddBlocks([]blocks.Block{block, bgen.Next()}); err != nil {
			t.Fatal(err)
		}
		if err := bserv.DeleteBlock(block.Cid()); err != nil {
			t.Fatal(err)
		}
		if err := bserv.AddBlocks([]blocks.Block{block, bgen.Next()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := bserv.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGetBlocks(t *testing.T) {
	bs := blockstore.NewBlockstore(
		zaptest.NewLogger(t), dssync.MutexWrap(ds.NewMapDatastore()),
	)
	exch := offline.Exchange(bs)
	bserv := New(bs, exch, zaptest.NewLogger(t))
	bgen := butil.NewBlockGenerator()
	var (
		cids    []cid.Cid
		wantCid = make(map[cid.Cid]bool)
	)
	for i := 0; i < 5; i++ {
		block := bgen.Next()
		if err := bserv.AddBlock(block); err != nil {
			t.Fatal(err)
		}
		cids = append(cids, block.Cid())
		wantCid[block.Cid()] = true
	}
	blocks := bserv.GetBlocks(context.Background(), cids)
	for block := range blocks {
		if !wantCid[block.Cid()] {
			t.Fatal("bad block count")
		}
	}
}

func TestWriteThroughWorks(t *testing.T) {
	bstore := &PutCountingBlockstore{
		blockstore.NewBlockstore(zaptest.NewLogger(t), dssync.MutexWrap(ds.NewMapDatastore())),
		0,
	}
	bstore2 := blockstore.NewBlockstore(zaptest.NewLogger(t), dssync.MutexWrap(ds.NewMapDatastore()))
	exch := offline.Exchange(bstore2)
	bserv := New(bstore, exch, zaptest.NewLogger(t))
	bgen := butil.NewBlockGenerator()

	block := bgen.Next()

	t.Logf("PutCounter: %d", bstore.PutCounter)
	err := bserv.AddBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	if bstore.PutCounter != 1 {
		t.Fatalf("expected just one Put call, have: %d", bstore.PutCounter)
	}

	err = bserv.AddBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	if bstore.PutCounter != 2 {
		t.Fatalf("Put should have called again, should be 2 is: %d", bstore.PutCounter)
	}

}

func TestLazySessionInitialization(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bstore := blockstore.NewBlockstore(zaptest.NewLogger(t), dssync.MutexWrap(ds.NewMapDatastore()))
	bstore2 := blockstore.NewBlockstore(zaptest.NewLogger(t), dssync.MutexWrap(ds.NewMapDatastore()))
	bstore3 := blockstore.NewBlockstore(zaptest.NewLogger(t), dssync.MutexWrap(ds.NewMapDatastore()))
	session := offline.Exchange(bstore2)
	exchange := offline.Exchange(bstore3)
	sessionExch := &fakeSessionExchange{Interface: exchange, session: session}
	bservSessEx := New(bstore, sessionExch, zaptest.NewLogger(t))
	bgen := butil.NewBlockGenerator()

	block := bgen.Next()
	err := bstore.Put(block)
	if err != nil {
		t.Fatal(err)
	}
	block2 := bgen.Next()
	err = session.HasBlock(block2)
	if err != nil {
		t.Fatal(err)
	}

	bsession := NewSession(ctx, bservSessEx, zaptest.NewLogger(t))
	if bsession.ses != nil {
		t.Fatal("Session exchange should not instantiated session immediately")
	}
	returnedBlock, err := bsession.GetBlock(ctx, block.Cid())
	if err != nil {
		t.Fatal("Should have fetched block locally")
	}
	if returnedBlock.Cid() != block.Cid() {
		t.Fatal("Got incorrect block")
	}
	if bsession.ses != nil {
		t.Fatal("Session exchange should not instantiated session if local store had block")
	}
	returnedBlock, err = bsession.GetBlock(ctx, block2.Cid())
	if err != nil {
		t.Fatal("Should have fetched block remotely")
	}
	if returnedBlock.Cid() != block2.Cid() {
		t.Fatal("Got incorrect block")
	}
	if bsession.ses != session {
		t.Fatal("Should have initialized session to fetch block")
	}
	block3 := bgen.Next()
	if err := bstore2.Put(block3); err != nil {
		t.Fatal(err)
	}
	block4 := bgen.Next()
	if err := bstore3.Put(block4); err != nil {
		t.Fatal(err)
	}
	blockChan := bsession.GetBlocks(
		ctx,
		[]cid.Cid{block.Cid(),
			block2.Cid(),
			block3.Cid(),
			block4.Cid(),
			// this shouldn't show up as it does not exist anywhere
			bgen.Next().Cid(),
		},
	)
	for blk := range blockChan {
		switch blk.Cid() {
		case block.Cid(), block2.Cid(), block3.Cid(), block4.Cid():
			continue
		default:
			t.Fatal("bad block found")
		}
	}
}

var _ blockstore.Blockstore = (*PutCountingBlockstore)(nil)

type PutCountingBlockstore struct {
	blockstore.Blockstore
	PutCounter int
}

func (bs *PutCountingBlockstore) Put(block blocks.Block) error {
	bs.PutCounter++
	return bs.Blockstore.Put(block)
}

func (bs *PutCountingBlockstore) PutMany(blks []blocks.Block) error {
	bs.PutCounter += len(blks)
	return bs.Blockstore.PutMany(blks)
}

var _ exchange.SessionExchange = (*fakeSessionExchange)(nil)

type fakeSessionExchange struct {
	exchange.Interface
	session exchange.Fetcher
}

func (fe *fakeSessionExchange) NewSession(ctx context.Context) exchange.Fetcher {
	if ctx == nil {
		panic("nil context")
	}
	return fe.session
}
