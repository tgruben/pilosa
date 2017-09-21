package plugins

import (
	"context"
	"errors"

	"github.com/pilosa/pilosa"
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
	n := 0
	var frame string

	args := call.Args

	if id, found := args["id"]; found {
		n = int(id.(int64))
	} else {
		return nil, errors.New("n required")
	}

	if fr, found := args["frame"]; found {
		frame = fr.(string)
	} else {
		return nil, errors.New("frame required")
	}

	view := p.holder.View(index, frame, pilosa.ViewStandard)
	f := view.Fragment(slice)

	var bm *pilosa.Bitmap
	//bm,err:=f.Load(id)
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
