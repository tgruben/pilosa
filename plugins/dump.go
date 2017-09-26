package plugins

import (
	"context"
	"errors"
	"fmt"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

func init() {
	pilosa.RegisterPlugin("Dump", NewDumpPlugin)
}

// DiffTopPlugin represents a plugin that will find the common bits of the top-n list.
type DumpPlugin struct {
	holder *pilosa.Holder
}

// NewDiffTopPlugin returns a new instance of DiffTopPlugin.
func NewDumpPlugin(e *pilosa.Executor) pilosa.Plugin {
	return &DumpPlugin{e.Holder}
}

// Map executes the plugin against a single slice.
func (p *DumpPlugin) Map(ctx context.Context, index string, call *pql.Call, slice uint64) (interface{}, error) {
	var frame string

	args := call.Args

	if fr, found := args["frame"]; found {
		frame = fr.(string)
	} else {
		return nil, errors.New("frame required")
	}

	view := p.holder.View(index, frame, pilosa.ViewStandard)
	f := view.Fragment(slice)
	ids := f.AllIds()

	return ids, nil
}

// Reduce combines previous map results into a single value.
func (p *DumpPlugin) Reduce(ctx context.Context, prev, v interface{}) interface{} {
	switch x := v.(type) {
	case []pilosa.Pair:
		if prev != nil {
			pairs := prev.([]pilosa.Pair)
			return pilosa.Pairs(pairs).Add(x)

		}
		return x
	case pilosa.Pairs:
		if prev != nil {
			switch p := prev.(type) {
			case []pilosa.Pair:
				return pilosa.Pairs(p).Add(x)

			case pilosa.Pairs:
				return p.Add(x)
			}
		}
		return x
	default:
		fmt.Println("WHAT", v)

	}
	return v
}

func (p *DumpPlugin) Decode(qr *internal.QueryResult) (interface{}, error) {
	return pilosa.DecodePairs(qr.GetPairs()), nil
}

func (p *DumpPlugin) Final() interface{} {
	return nil
}
