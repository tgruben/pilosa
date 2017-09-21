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
	holder *pilosa.Holder
}

// NewDiffTopPlugin returns a new instance of DiffTopPlugin.
func NewStorePlugin(e *pilosa.Executor) pilosa.Plugin {
	return &StorePlugin{e.Holder}
}

// Map executes the plugin against a single slice.
func (p *StorePlugin) Map(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error) {
	n := 0
	var frame string

	args := call.Args

	if id, found := args["id"]; found {
		n = int(id.(int64))
	} else {
		return nil, errors.New("n required")
	}

	view := p.holder.View(index, frame, pilosa.ViewRegister)
	f := view.Fragment(slice)

	//GetOrCreateRegisterFrame
	// call to build bitmap from children1 ..
	//Store the results
	var bm *pilosa.Bitmap
	//bm,err:=f.Store(id,bm)
	return infoAboutRegistry, nil
}

// Reduce combines previous map results into a single value.
func (p *StorePlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {

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
