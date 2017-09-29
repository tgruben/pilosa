package plugins

import (
	"context"
	"errors"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

func init() {
	pilosa.RegisterPlugin("Negate", NewNegatePlugin)
}

// NegatePlugin represents a plugin that will find the common bits of the top-n list.
type NegatePlugin struct {
	executor *pilosa.Executor
}

// NewNegatePlugin returns a new instance of DiffTopPlugin.
func NewNegatePlugin(e *pilosa.Executor) pilosa.Plugin {
	return &NegatePlugin{e}
}

// Map executes the plugin against a single slice.
func (p *NegatePlugin) Map(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error) {

	child, err := p.executor.ExecuteBitmapCallSlice(ctx, index, call, slice)
	/*
			idx := p.executor.Holder.Index(index)
			if idx == nil {
				return nil, pilosa.ErrIndexNotFound
			}

		maxSlice := idx.MaxSlice()
	*/

	bm := pilosa.NewBitmap()
	bm.Flip()
	if err != nil {
		return nil, errors.New("Invalid Bitmap Argument")
	} else {
		bm.CopyFrom(child)

	}

	return child, nil

}

// Reduce combines previous map results into a single value.
func (p *NegatePlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {
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

func (p *NegatePlugin) Decode(qr *internal.QueryResult) (interface{}, error) {
	return pilosa.DecodeBitmap(qr.GetBitmap()), nil
}

func (p *NegatePlugin) Final() interface{} {
	return nil
}
