package plugins

import (
	"context"
	"errors"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

func init() {
	pilosa.RegisterPlugin("Load", NewLoadPlugin)
}

// LoadPlugin represents a plugin that will read bitmaps from a register
type LoadPlugin struct {
	holder *pilosa.Holder
}

// NewDiffTopPlugin returns a new instance of DiffTopPlugin.
func NewLoadPlugin(e *pilosa.Executor) pilosa.Plugin {
	return &LoadPlugin{e.Holder}
}

// Map executes the plugin against a single slice.
func (p *LoadPlugin) Map(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error) {

	args := call.Args
	id := uint64(0)
	if ids, found := args["id"]; found {
		id = uint64(ids.(int64))
	} else {
		return nil, errors.New("id required")
	}
	bm, _ := p.holder.Load(index, slice, id)
	return bm, nil
}

// Reduce combines previous map results into a single value.
func (p *LoadPlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {

	switch x := v.(type) {
	case *pilosa.Bitmap:
		if prev != nil {
			bm := prev.(*pilosa.Bitmap)
			return bm.Union(x)
		}
		return x
	case int:
		return x
	}

	return v
}

func (p *LoadPlugin) Decode(*internal.QueryResult) (interface{}, error) {

	return nil, nil
}
func (p *LoadPlugin) Final() interface{} {
	return nil
}
