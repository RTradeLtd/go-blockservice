package blockservice

import (
	"context"
	"sync"

	blockstore "github.com/RTradeLtd/go-ipfs-blockstore/v2"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"go.uber.org/zap"
)

var _ BlockGetter = (*Session)(nil)

// Session is a helper type to provide higher level access to bitswap sessions
type Session struct {
	bs      blockstore.Blockstore
	ses     exchange.Fetcher
	sessEx  exchange.SessionExchange
	sessCtx context.Context
	lk      sync.Mutex
	logger  *zap.Logger
}

// NewSession creates a new session that allows for
// controlled exchange of wantlists to decrease the bandwidth overhead.
// If the current exchange is a SessionExchange, a new exchange
// session will be created. Otherwise, the current exchange will be used
// directly.
func NewSession(ctx context.Context, bs BlockService, logger *zap.Logger) *Session {
	exch := bs.Exchange()
	if sessEx, ok := exch.(exchange.SessionExchange); ok {
		return &Session{
			sessCtx: ctx,
			ses:     nil,
			sessEx:  sessEx,
			bs:      bs.Blockstore(),
			logger:  logger.Named("blockservice.session"),
		}
	}
	return &Session{
		ses:     exch,
		sessCtx: ctx,
		bs:      bs.Blockstore(),
		logger:  logger.Named("blockservice.session"),
	}
}

func (s *Session) getSession() exchange.Fetcher {
	s.lk.Lock()
	defer s.lk.Unlock()
	if s.ses == nil {
		s.ses = s.sessEx.NewSession(s.sessCtx)
	}

	return s.ses
}

// GetBlock gets a block in the context of a request session
func (s *Session) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return getBlock(ctx, c, s.bs, s.getSession)
}

// GetBlocks gets blocks in the context of a request session
func (s *Session) GetBlocks(ctx context.Context, ks []cid.Cid) <-chan blocks.Block {
	return getBlocks(ctx, ks, s.bs, s.getSession, s.logger)
}
