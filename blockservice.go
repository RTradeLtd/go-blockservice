// package blockservice implements a BlockService interface that provides
// a single GetBlock/AddBlock interface that seamlessly retrieves data either
// locally or from a remote peer through the exchange.
package blockservice

import (
	"context"
	"errors"
	"io"

	blockstore "github.com/RTradeLtd/go-ipfs-blockstore/v2"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"go.uber.org/zap"
)

var (
	// ErrNotFound is returned when the blockservice is unable to find the key
	ErrNotFound = errors.New("blockservice: key not found")
)

// BlockGetter is the common interface shared between blockservice sessions and
// the blockservice.
type BlockGetter interface {
	// GetBlock gets the requested block.
	GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error)

	// GetBlocks does a batch request for the given cids, returning blocks as
	// they are found, in no particular order.
	//
	// It may not be able to find all requested blocks (or the context may
	// be canceled). In that case, it will close the channel early. It is up
	// to the consumer to detect this situation and keep track which blocks
	// it has received and which it hasn't.
	GetBlocks(ctx context.Context, ks []cid.Cid) <-chan blocks.Block
}

// BlockService is a hybrid block datastore. It stores data in a local
// datastore and may retrieve data from a remote Exchange.
// It uses an internal `datastore.Datastore` instance to store values.
type BlockService interface {
	io.Closer
	BlockGetter

	// Blockstore returns a reference to the underlying blockstore
	Blockstore() blockstore.Blockstore

	// Exchange returns a reference to the underlying exchange (usually bitswap)
	Exchange() exchange.Interface

	// AddBlock puts a given block to the underlying datastore
	AddBlock(o blocks.Block) error

	// AddBlocks adds a slice of blocks at the same time using batching
	// capabilities of the underlying datastore whenever possible.
	AddBlocks(bs []blocks.Block) error

	// DeleteBlock deletes the given block from the blockservice.
	DeleteBlock(o cid.Cid) error
}

type blockService struct {
	blockstore blockstore.Blockstore
	exchange   exchange.Interface
	logger     *zap.Logger
}

// New creates a BlockService with given datastore instance.
func New(bs blockstore.Blockstore, rem exchange.Interface, logger *zap.Logger) BlockService {
	if rem == nil {
		logger.Debug("blockservice running in local (offline) mode")
	}

	return &blockService{
		blockstore: bs,
		exchange:   rem,
		logger:     logger.Named("blockservice"),
	}
}

// Blockstore returns the blockstore behind this blockservice.
func (s *blockService) Blockstore() blockstore.Blockstore {
	return s.blockstore
}

// Exchange returns the exchange behind this blockservice.
func (s *blockService) Exchange() exchange.Interface {
	return s.exchange
}

// AddBlock adds a particular block to the service, Putting it into the datastore.
// TODO pass a context into this if the remote.HasBlock is going to remain here.
func (s *blockService) AddBlock(o blocks.Block) error {
	var doAnnounce bool
	if has, err := s.blockstore.Has(o.Cid()); err != nil {
		return err
	} else if !has {
		// only announce if we do not have
		doAnnounce = true
	}

	if err := s.blockstore.Put(o); err != nil {
		return err
	}

	if s.exchange != nil && doAnnounce {
		if err := s.exchange.HasBlock(o); err != nil {
			s.logger.Error("HasBlock failed", zap.Error(err))
		}
	}

	return nil
}

func (s *blockService) AddBlocks(bs []blocks.Block) error {
	var toAnnounce = make([]blocks.Block, 0, len(bs))
	for _, b := range bs {
		if has, err := s.blockstore.Has(b.Cid()); err != nil {
			return err
		} else if !has {
			// only announce if we do not have
			toAnnounce = append(toAnnounce, b)
		}
	}

	if err := s.blockstore.PutMany(bs); err != nil {
		return err
	}

	if s.exchange != nil {
		for _, o := range toAnnounce {
			if err := s.exchange.HasBlock(o); err != nil {
				s.logger.Error("HasBlock failed", zap.Error(err))
			}
		}
	}
	return nil
}

// GetBlock retrieves a particular block from the service,
// Getting it from the datastore using the key (hash).
func (s *blockService) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	var f func() exchange.Fetcher
	if s.exchange != nil {
		f = s.getExchange
	}
	return getBlock(ctx, c, s.blockstore, f)
}

func (s *blockService) getExchange() exchange.Fetcher {
	return s.exchange
}

func getBlock(ctx context.Context, c cid.Cid, bs blockstore.Blockstore, fget func() exchange.Fetcher) (blocks.Block, error) {
	block, err := bs.Get(c)
	if err == nil {
		return block, nil
	}

	if err == blockstore.ErrNotFound && fget != nil {
		// TODO be careful checking ErrNotFound. If the underlying
		// implementation changes, this will break.
		blk, err := fget().GetBlock(ctx, c)
		if err != nil && err == blockstore.ErrNotFound {
			return nil, ErrNotFound
		} else if err != nil {
			return nil, err
		}
		return blk, nil
	}
	if err == blockstore.ErrNotFound {
		return nil, ErrNotFound
	}

	return nil, err
}

// GetBlocks gets a list of blocks asynchronously and returns through
// the returned channel.
// NB: No guarantees are made about order.
func (s *blockService) GetBlocks(ctx context.Context, ks []cid.Cid) <-chan blocks.Block {
	var f func() exchange.Fetcher
	if s.exchange != nil {
		f = s.getExchange
	}

	return getBlocks(ctx, ks, s.blockstore, f, s.logger)
}

func getBlocks(ctx context.Context, ks []cid.Cid, bs blockstore.Blockstore, fget func() exchange.Fetcher, logger *zap.Logger) <-chan blocks.Block {
	out := make(chan blocks.Block)

	go func() {
		defer close(out)
		var misses []cid.Cid
		for _, c := range ks {
			hit, err := bs.Get(c)
			if err != nil {
				misses = append(misses, c)
				continue
			}
			select {
			case out <- hit:
			case <-ctx.Done():
				return
			}
		}

		if len(misses) == 0 || fget == nil {
			return
		}

		rblocks, err := fget().GetBlocks(ctx, misses)
		if err != nil {
			logger.Debug("failed to get blocks", zap.Error(err))
			return
		}

		for b := range rblocks {
			select {
			case out <- b:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// DeleteBlock deletes a block in the blockservice from the datastore
func (s *blockService) DeleteBlock(c cid.Cid) error {
	return s.blockstore.DeleteBlock(c)
}

func (s *blockService) Close() error {
	return s.exchange.Close()
}
