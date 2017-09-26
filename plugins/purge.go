package plugins

import (
	"context"
	"errors"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

func init() {
	pilosa.RegisterPlugin("Purge", NewPurgePlugin)
}

// LoadPlugin represents a plugin that will read bitmaps from a register
type PurgePlugin struct {
	holder *pilosa.Holder
}

// NewPurgePlugin returns a new instance of DiffTopPlugin.
func NewPurgePlugin(e *pilosa.Executor) pilosa.Plugin {
	return &PurgePlugin{e.Holder}
}

// Map executes the plugin against a single slice.
func (p *PurgePlugin) Map(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error) {
	args := call.Args
	id := uint64(0)
	if idi, found := args["id"]; found {
		id = uint64(idi.(int64))
	} else {
		return nil, errors.New("id required")
	}

	p.holder.Purge(index, slice, id)
	return "", nil
}

// Reduce combines previous map results into a single value.
func (p *PurgePlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {

	return v
}

func (p *PurgePlugin) Decode(qr *internal.QueryResult) (interface{}, error) {
	return nil, nil
}

func (p *PurgePlugin) Final() interface{} {
	return nil
}
