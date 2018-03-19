package pilosa

import (
	"archive/tar"
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pilosa/pilosa/roaring"
)

const (
	rowBits = 20
	hotLen  = 4
)

func Pos(rowID, columnID uint64) uint64 {
	return (rowID * SliceWidth) + (columnID % SliceWidth)
}

type Position struct {
	Row, Col uint64
}

func (p *Position) ToBit() uint64 {
	return (p.Row * SliceWidth) + (p.Col % SliceWidth)
}

type BitIterator interface {
	Next() (uint64, bool)
	Seek(offset uint64)
	Peek() *roaring.Bitmap
}

type StorageBackend interface {
	Add(pos *Position) (bool, error)
	Remove(pos *Position) (bool, error)
	Contains(pos *Position) bool
	Count(row uint64) uint64
	Row(row uint64) *Bitmap
	ForEachBit(fn func(rowID, columnID uint64) error) error
	Open() error
	Flush() error
	Close() error
	Max() uint64
	Iterator() BitIterator
	WriteToArchive(tw *tar.Writer) error
	ReadFromArchive(r io.Reader, path string) error
	BlockData(id int) (rowIDs, columnIDs []uint64)
	Path() string
}

type FileStorage struct {
	path        string
	file        *os.File
	storage     *roaring.Bitmap
	storageData []byte
	slice       uint64
	// Writer used for out-of-band log entries.
	LogOutput io.Writer
	opN       int
	maxOpN    int
	stats     StatsClient
}

func NewFileStorage(path string, slice uint64) StorageBackend {
	return &FileStorage{
		path:      path,
		storage:   roaring.NewBitmap(),
		slice:     slice,
		maxOpN:    DefaultFragmentMaxOpN,
		stats:     NopStatsClient,
		LogOutput: ioutil.Discard,
	}
}

func (fs *FileStorage) Add(pos *Position) (changed bool, err error) {
	changed, err = fs.storage.Add(pos.ToBit())
	if !changed || err != nil {
		return
	}
	// Increment number of operations until snapshot is required.
	if err = fs.incrementOpN(); err != nil {
		return
	}
	return
}

func (fs *FileStorage) Remove(pos *Position) (changed bool, err error) {
	changed, err = fs.storage.Remove(pos.ToBit())
	if !changed || err != nil {
		return
	}
	// Increment number of operations until snapshot is required.
	if err = fs.incrementOpN(); err != nil {
		return
	}
	return
}

func (fs *FileStorage) Contains(pos *Position) bool {
	return fs.storage.Contains(pos.ToBit())
}

func (fs *FileStorage) Count(row uint64) uint64 {
	return fs.storage.CountRange(row*SliceWidth, (row+1)*SliceWidth)
}
func (fs *FileStorage) Max() uint64 {
	return fs.storage.Max()
}

func (fs *FileStorage) Row(rowID uint64) *Bitmap {
	// NOTE: The start & end ranges must be divisible by
	data := fs.storage.OffsetRange(fs.slice*SliceWidth, rowID*SliceWidth, (rowID+1)*SliceWidth)
	// Reference bitmap subrange in storage.
	// We Clone() data because otherwise bm will contains pointers to containers in storage.
	// This causes unexpected results when we cache the row and try to use it later.
	bm := &Bitmap{
		segments: []BitmapSegment{{
			data:     *data.Clone(),
			slice:    fs.slice,
			writable: false,
		}},
	}
	bm.InvalidateCount()
	return bm
}
func (fs *FileStorage) ForEachBit(fn func(rowID, columnID uint64) error) error {
	var err error
	fs.storage.ForEach(func(i uint64) {
		// Skip if an error has already occurred.
		if err != nil {
			return
		}

		// Invoke caller's function.
		err = fn(i/SliceWidth, (fs.slice*SliceWidth)+(i%SliceWidth))
	})
	return err
}

func (fs *FileStorage) Open() error {
	// Open the data file to be mmap'd and used as an ops log.
	file, err := os.OpenFile(fs.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("open file: %s", err)
	}
	fs.file = file

	// Lock the underlying file.
	if err := syscall.Flock(int(fs.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("flock: %s", err)
	}

	// If the file is empty then initialize it with an empty bitmap.
	fi, err := fs.file.Stat()
	if err != nil {
		return err
	} else if fi.Size() == 0 {
		bi := bufio.NewWriter(fs.file)
		if _, err := fs.storage.WriteTo(bi); err != nil {
			return fmt.Errorf("init storage file: %s", err)
		}
		bi.Flush()
		fi, err = fs.file.Stat()
		if err != nil {
			return err
		}
	}

	// Mmap the underlying file so it can be zero copied.
	storageData, err := syscall.Mmap(int(fs.file.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %s", err)
	}
	fs.storageData = storageData

	// Advise the kernel that the mmap is accessed randomly.
	if err := madvise(fs.storageData, syscall.MADV_RANDOM); err != nil {
		return fmt.Errorf("madvise: %s", err)
	}

	// Attach the mmap file to the bitmap.
	data := fs.storageData
	if err := fs.storage.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("unmarshal storage: file=%s, err=%s", fs.file.Name(), err)
	}
	fs.opN = 0
	fs.storage.OpWriter = fs.file

	return nil
}

func (fs *FileStorage) Close() error {
	// Unmap the file.
	if fs.storageData != nil {
		if err := syscall.Munmap(fs.storageData); err != nil {
			return fmt.Errorf("munmap: %s", err)
		}
		fs.storageData = nil
	}

	// Flush file, unlock & close.
	if fs.file != nil {
		if err := fs.file.Sync(); err != nil {
			return fmt.Errorf("sync: %s", err)
		}
		if err := syscall.Flock(int(fs.file.Fd()), syscall.LOCK_UN); err != nil {
			return fmt.Errorf("unlock: %s", err)
		}
		if err := fs.file.Close(); err != nil {
			return fmt.Errorf("close file: %s", err)
		}
	}

	return nil
}

func track(start time.Time, message string, stats StatsClient, logger *log.Logger) {
	elapsed := time.Since(start)
	stats.Histogram("snapshot", elapsed.Seconds(), 1.0)
}
func (fs *FileStorage) Flush() error {
	logger := fs.logger()
	logger.Printf("fragment: snapshotting %s/%d", fs.path, fs.slice)
	completeMessage := fmt.Sprintf("fragment: snapshot complete %s/%d", fs.path, fs.slice)
	start := time.Now()
	defer track(start, completeMessage, fs.stats, logger)

	// Create a temporary file to snapshot to.
	snapshotPath := fs.path + SnapshotExt
	file, err := os.Create(snapshotPath)
	if err != nil {
		return fmt.Errorf("create snapshot file: %s", err)
	}
	defer file.Close()

	// Write storage to snapshot.
	bw := bufio.NewWriter(file)
	if _, err := fs.storage.WriteTo(bw); err != nil {
		return fmt.Errorf("snapshot write to: %s", err)
	}

	if err := bw.Flush(); err != nil {
		return fmt.Errorf("flush: %s", err)
	}

	// Close current storage.
	if err := fs.Close(); err != nil {
		return fmt.Errorf("close storage: %s", err)
	}

	// Move snapshot to data file location.
	if err := os.Rename(snapshotPath, fs.path); err != nil {
		return fmt.Errorf("rename snapshot: %s", err)
	}

	// Reopen storage.
	if err := fs.Open(); err != nil {
		return fmt.Errorf("open storage: %s", err)
	}

	// Reset operation count.
	fs.opN = 0

	return nil
}

func (fs *FileStorage) incrementOpN() error {
	fs.opN++
	if fs.opN <= fs.maxOpN {
		return nil
	}

	if err := fs.Flush(); err != nil {
		return fmt.Errorf("snapshot: %s", err)
	}
	return nil
}

func (fs *FileStorage) WriteToArchive(tw *tar.Writer) error {
	// Open separate file descriptor to read from.
	//debug.PrintStack()
	file, err := os.Open(fs.path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Retrieve the current file size under lock so we don't read
	// while an operation is appending to the end.
	var sz int64
	if err := func() error {
		fi, err := file.Stat()
		if err != nil {
			return err
		}
		sz = fi.Size()

		return nil
	}(); err != nil {
		return err
	}

	// Write archive header.
	if err := tw.WriteHeader(&tar.Header{
		//Name: "data",
		Name:    path.Base(fs.path),
		Mode:    0600,
		Size:    sz,
		ModTime: time.Now(),
	}); err != nil {
		return err
	}

	// Copy the file up to the last known size.
	// This is done outside the lock because the storage format is append-only.
	if _, err := io.CopyN(tw, file, sz); err != nil {
		return err
	}
	return nil
}
func (fs *FileStorage) BlockData(id int) (rowIDs, columnIDs []uint64) {

	fs.storage.ForEachRange(uint64(id)*HashBlockSize*SliceWidth, (uint64(id)+1)*HashBlockSize*SliceWidth, func(i uint64) {
		rowIDs = append(rowIDs, i/SliceWidth)
		columnIDs = append(columnIDs, i%SliceWidth)
	})
	return
}

func (fs *FileStorage) ReadFromArchive(r io.Reader, pt string) error {
	// Create a temporary file to copy into.
	path := fs.path + CopyExt
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Copy reader into temporary path.
	if _, err = io.Copy(file, r); err != nil {
		return err
	}

	// Close current storage.
	if err := fs.Close(); err != nil {
		return err
	}

	// Move snapshot to data file location.
	if err := os.Rename(path, fs.path); err != nil {
		return err
	}

	// Reopen storage.
	if err := fs.Open(); err != nil {
		return err
	}

	return nil
}

func (fs *FileStorage) logger() *log.Logger {
	return log.New(fs.LogOutput, "", log.LstdFlags)
}

func (fs *FileStorage) Iterator() BitIterator {
	return fs.storage.Iterator()
}

func (fs *FileStorage) Path() string {
	return fs.path
}

///

func (ps *PagedStorage) Add(pos *Position) (bool, error) {
	page := ps.getPage(pos.Row)
	return page.Add(pos)

}

func (ps *PagedStorage) Remove(pos *Position) (bool, error) {
	page := ps.getPage(pos.Row)
	return page.Remove(pos)
}

func (ps *PagedStorage) Contains(pos *Position) bool {
	page := ps.getPage(pos.Row)
	return page.Contains(pos)
}

func (ps *PagedStorage) Count(row uint64) uint64 {
	page := ps.getPage(row)
	return page.Count(row)
}
func (ps *PagedStorage) Row(row uint64) *Bitmap {
	page := ps.getPage(row)
	return page.Row(row)
}
func (ps *PagedStorage) ForEachBit(fn func(rowID, columnID uint64) error) error {
	for page := range ps.All() {
		e := page.storage.ForEachBit(fn)
		if e != nil {
			return e
		}
	}
	return nil
}

//ignore the open
func (ps *PagedStorage) Open() error {
	return nil
}

//only snapshot the last page used
//may need to change to only the last page used
func (ps *PagedStorage) Flush() (err error) {
	for i := range ps.hot {
		ps.hot[i].storage.Flush()
	}
	return
}
func (ps *PagedStorage) Close() error {
	for i := range ps.hot {
		ps.hot[i].storage.Close()
	}
	return nil
}

//should just open last
func (ps *PagedStorage) Max() uint64 {
	max := uint64(0)
	for page := range ps.All() {
		if max < page.Max() {
			max = page.Max()
		}
	}
	return max
}
func (ps *PagedStorage) Iterator() BitIterator {
	pi := NewPagedIterator()
	for page := range ps.All() {
		pi.Push(page.storage.Iterator())
	}
	return pi
}
func (ps *PagedStorage) WriteToArchive(tw *tar.Writer) (err error) {
	for page := range ps.All() {
		err = page.storage.WriteToArchive(tw)
	}
	return
}
func (ps *PagedStorage) ReadFromArchive(r io.Reader, path string) (err error) {
	t := strings.Split(path, ".")
	row, _ := strconv.ParseUint(t[1], 10, 64)
	rowSlice := row << rowBits
	page := ps.getPage(rowSlice)
	err = page.storage.ReadFromArchive(r, path)
	return
}

func (ps *PagedStorage) BlockData(blockid int) (rowIDs, columnIDs []uint64) {
	//blockid = v/(hashbocksize*slicewidth)
	originalV := uint64(blockid) * HashBlockSize * SliceWidth
	rowSlice := originalV >> rowBits
	if ps.slice != 0 {
		rowSlice = (originalV / ps.slice) >> rowBits
	}
	page := ps.getPage(rowSlice)
	return page.storage.BlockData(blockid)

}

func (ps *PagedStorage) Path() string {
	return ps.path
}

func NewPagedStorage(path string, slice uint64) StorageBackend {
	return &PagedStorage{
		slice: slice,
		path:  path,
	}
}

type PagedStorage struct {
	hot   []*Page
	slice uint64
	path  string
}

type Page struct {
	storage  StorageBackend
	rowSlice uint64
}

func (p *Page) Add(pos *Position) (bool, error) {
	return p.storage.Add(pos)
}

func (p *Page) Remove(pos *Position) (bool, error) {
	return p.storage.Remove(pos)
}

func (p *Page) Contains(pos *Position) bool {
	return p.storage.Contains(pos)
}

func (p *Page) Count(row uint64) uint64 {
	return p.storage.Count(row)
}

func (p *Page) Row(row uint64) *Bitmap {
	return p.storage.Row(row)
}

func (p *Page) Max() uint64 {
	return p.storage.Max()
}

func (ps *PagedStorage) openPage(rowSlice uint64) *Page {
	path := fmt.Sprintf("%s.%d.page", ps.Path(), rowSlice)
	p := &Page{
		storage:  NewFileStorage(path, ps.slice),
		rowSlice: rowSlice,
	}
	p.storage.Open()

	return p
}

func (ps *PagedStorage) getPage(row uint64) *Page {
	rowSlice := row >> rowBits

	page, found := ps.find(rowSlice)
	if !found {
		page = ps.openPage(rowSlice)
		//ps.hot[rowSlice] = page
	}
	ps.used(page)
	return page
}

func (ps *PagedStorage) find(rowSlice uint64) (*Page, bool) {
	for i := range ps.hot {
		if rowSlice == ps.hot[i].rowSlice {
			return ps.hot[i], true
		}
	}
	return nil, false
}

func (ps *PagedStorage) used(page *Page) {
	//only add if different from last paged added
	if len(ps.hot) > 0 {
		if ps.hot[len(ps.hot)-1].rowSlice != page.rowSlice {
			ps.hot = append(ps.hot, page)
		}
	} else {
		ps.hot = append(ps.hot, page)
	}
	//trim if cache to big
	if len(ps.hot) > hotLen {
		ps.hot = append(ps.hot[:0], ps.hot[:1]...)
	}
}

func extractRowSlice(path string) uint64 {
	work := filepath.Base(path)
	parts := strings.Split(work, ".")
	i, _ := strconv.ParseUint(parts[1], 10, 64)

	return i
}

func (ps *PagedStorage) All() chan *Page {
	returnChan := make(chan *Page)
	go func() {
		matchString := fmt.Sprintf("%s.*.page", ps.path)
		filepath.Walk(filepath.Dir(ps.path),
			func(path string, f os.FileInfo, err error) error {

				ok, err := filepath.Match(matchString, path)
				if ok {
					rowSlice := extractRowSlice(path)
					returnChan <- ps.getPage(rowSlice)
				}
				return nil
			})
		close(returnChan)
	}()

	return returnChan
}

func NewPagedIterator() *PagedIterator {
	return &PagedIterator{}
}

type PagedIterator struct {
	items []BitIterator
}

func (pi *PagedIterator) Push(item BitIterator) {
	temp := []BitIterator{item}
	pi.items = append(temp, pi.items...)
}

func (pi *PagedIterator) Pop() BitIterator {
	defer func() {
		pi.items = pi.items[1:]
	}()
	return pi.items[0]
}

func (pi *PagedIterator) IsEmpty() bool {
	if len(pi.items) == 0 {
		return true
	}
	return false
}

func (pi *PagedIterator) Next() (uint64, bool) {
	for !pi.IsEmpty() {
		v, eof := pi.items[0].Next()
		if !eof {
			return v, eof
		}
		pi.Pop()
	}
	return 0, true
}
func (pi *PagedIterator) Peek() *roaring.Bitmap {
	if !pi.IsEmpty() {
		return pi.items[0].Peek()
	}
	return nil
}

func (pi *PagedIterator) Seek(offset uint64) {

	for len(pi.items) > 0 {
		if offset > pi.items[0].Peek().Max() {
			pi.Pop()

		} else {
			pi.items[0].Seek(offset)
			return
		}
	}
}
