package chain

import "github.com/pkg/errors"

type ChainGroup struct {
	chains []*ExpresionChain
}

func (cg *ChainGroup) Add(ec *ExpresionChain) {
	cg.chains = append(cg.chains, ec)
}

func (cg *ChainGroup) Run() error {
	if len(cg.chains) == 0 {
		return nil
	}
	for _, op := range cg.chains {
		if op.mainOperation.segment == sqlSelect {
			return errors.Errorf("cannot query as part of a chain.")
		}
	}
	// TODO actually run, begin tx and run execs and eventually just commit or rollback
	return nil
}
