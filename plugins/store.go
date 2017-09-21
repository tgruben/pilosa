package plugins

import (
	"context"
	"errors"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
)

func init() {
	pilosa.RegisterPlugin("Store", NewStorePlugin)
}

// LoadPlugin represents a plugin that will read bitmaps from a register
type StorePlugin struct {
	executor *pilosa.Executor
}

// NewStorePlugin returns a new instance of StorePlugin.
func NewStorePlugin(e *pilosa.Executor) pilosa.Plugin {
	return &StorePlugin{e}
}

// Map executes the plugin against a single slice.
func (p *StorePlugin) Map(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error) {
	//var frame string

	args := call.Args
	id := uint64(0)
	if idi, found := args["id"]; found {
		id = uint64(idi.(int64))
	} else {
		return nil, errors.New("id required")
	}
	bm := pilosa.NewBitmap()
	child, _ := p.executor.ExecuteCallSlice(ctx, index, call.Children[0], slice, p)
	switch v := child.(type) {
	case *pilosa.Bitmap: //handle bitmaps

		bm.CopyFrom(v)
		p.executor.Holder.Store(index, slice, id, bm)
		return "OK", nil
	}
	return nil, errors.New("Invalid Bitmap Argument")

}

// Reduce combines previous map results into a single value.
func (p *StorePlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {

	return v
}
