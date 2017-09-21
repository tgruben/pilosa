package pilosa

import (
	"io"

	"github.com/pilosa/pilosa/pql"
)

type View interface {
	Name() string
	Index() string
	Frame() string
	Path() string
	Open() error
	Close() error
	MaxSlice() uint64
	FragmentPath(slice uint64) string
	Fragment(slice uint64) *Fragment
	fragment(slice uint64) *Fragment
	Fragments() []*Fragment
	CreateFragmentIfNotExists(slice uint64) (*Fragment, error)
	createFragmentIfNotExists(slice uint64) (*Fragment, error)
	SetBit(rowID, columnID uint64) (changed bool, err error)
	ClearBit(rowID, columnID uint64) (changed bool, err error)
	FieldValue(columnID uint64, bitDepth uint) (value uint64, exists bool, err error)
	SetFieldValue(columnID uint64, bitDepth uint, value uint64) (changed bool, err error)
	FieldSum(filter *Bitmap, bitDepth uint) (sum, count uint64, err error)
	FieldRange(op pql.Token, bitDepth uint, predicate uint64) (*Bitmap, error)
	FieldRangeBetween(bitDepth uint, predicateMin, predicateMax uint64) (*Bitmap, error)
	SetRowAttrStore(*AttrStore)
	SetCacheType(string)
	SetLogOutput(io.Writer)
	SetStats(StatsClient)
	SetBroadcaster(Broadcaster)
}
