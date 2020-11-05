package block

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
)

type printChangeLog interface {
	DeleteSeries(del labels.Labels, intervals tombstones.Intervals)
	ModifySeries(old labels.Labels, new labels.Labels)
}

type changeLog struct {
	w io.Writer
}

func (l *changeLog) DeleteSeries(del labels.Labels, intervals tombstones.Intervals) {
	_, _ = fmt.Fprintf(l.w, "Deleted %v %v\n", del.String(), intervals)
}

func (l *changeLog) ModifySeries(old labels.Labels, new labels.Labels) {
	_, _ = fmt.Fprintf(l.w, "Relabelled %v %v\n", old.String(), new.String())
}

type seriesWriter struct {
	tmpDir string
	logger log.Logger

	chunkPool    chunkenc.Pool
	changeLogger printChangeLog

	dryRun bool
}

type seriesReader struct {
	ir tsdb.IndexReader
	cr tsdb.ChunkReader
}

func NewSeriesWriter(tmpDir string, logger log.Logger, changeLogger printChangeLog, pool chunkenc.Pool) *seriesWriter {
	return &seriesWriter{
		tmpDir:       tmpDir,
		logger:       logger,
		changeLogger: changeLogger,
		chunkPool:    pool,
	}
}

// TODO(bwplotka): Upstream this.
func (w *seriesWriter) WriteSeries(ctx context.Context, readers []Reader, sWriter Writer, modifiers ...Modifier) (err error) {
	if len(readers) == 0 {
		return errors.New("cannot write from no readers")
	}

	var (
		sReaders []seriesReader
		closers  []io.Closer
	)
	defer func() {
		errs := tsdb_errors.NewMulti(err)
		if cerr := tsdb_errors.CloseAll(closers); cerr != nil {
			errs.Add(errors.Wrap(cerr, "close"))
		}
		err = errs.Err()
	}()

	for _, b := range readers {
		indexr, err := b.Index()
		if err != nil {
			return errors.Wrapf(err, "open index reader for block %+v", b.Meta())
		}
		closers = append(closers, indexr)

		chunkr, err := b.Chunks()
		if err != nil {
			return errors.Wrapf(err, "open chunk reader for block %+v", b.Meta())
		}
		closers = append(closers, chunkr)
		sReaders = append(sReaders, seriesReader{ir: indexr, cr: chunkr})
	}

	symbols, set, err := compactSeries(ctx, sReaders...)
	if err != nil {
		return errors.Wrapf(err, "compact series from %v", func() string {
			var metas []string
			for _, m := range readers {
				metas = append(metas, fmt.Sprintf("%v", m.Meta()))
			}
			return strings.Join(metas, ",")
		}())
	}

	for _, m := range modifiers {
		symbols, set = m.Modify(symbols, set, w.changeLogger)
	}

	if w.dryRun {
		return nil
	}

	if err := w.write(ctx, symbols, set, sWriter); err != nil {
		return errors.Wrap(err, "write")
	}
	return nil
}

// compactSeries compacts blocks' series into symbols and one ChunkSeriesSet with lazy populating chunks.
func compactSeries(ctx context.Context, sReaders ...seriesReader) (symbols index.StringIter, set storage.ChunkSeriesSet, _ error) {
	if len(sReaders) == 0 {
		return nil, nil, errors.New("cannot populate block from no readers")
	}

	var sets []storage.ChunkSeriesSet
	for i, r := range sReaders {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}

		k, v := index.AllPostingsKey()
		all, err := r.ir.Postings(k, v)
		if err != nil {
			return nil, nil, err
		}
		all = r.ir.SortedPostings(all)
		syms := r.ir.Symbols()
		sets = append(sets, newLazyPopulateChunkSeriesSet(r, all))
		if i == 0 {
			symbols = syms
			set = sets[0]
			continue
		}
		symbols = tsdb.NewMergedStringIter(symbols, syms)
	}

	if len(sets) <= 1 {
		return symbols, set, nil
	}
	// Merge series using compacting chunk series merger.
	return symbols, storage.NewMergeChunkSeriesSet(sets, storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge)), nil
}

type lazyPopulateChunkSeriesSet struct {
	sReader seriesReader

	all index.Postings

	bufChks []chunks.Meta
	bufLbls labels.Labels

	curr *storage.ChunkSeriesEntry
	err  error
}

func newLazyPopulateChunkSeriesSet(sReader seriesReader, all index.Postings) *lazyPopulateChunkSeriesSet {
	return &lazyPopulateChunkSeriesSet{sReader: sReader, all: all}
}

func (s *lazyPopulateChunkSeriesSet) Next() bool {
	for s.all.Next() {
		if err := s.sReader.ir.Series(s.all.At(), &s.bufLbls, &s.bufChks); err != nil {
			// Postings may be stale. Skip if no underlying series exists.
			if errors.Cause(err) == storage.ErrNotFound {
				continue
			}
			s.err = errors.Wrapf(err, "get series %d", s.all.At())
			return false
		}

		if len(s.bufChks) == 0 {
			continue
		}

		for i := range s.bufChks {
			s.bufChks[i].Chunk = &lazyPopulatableChunk{cr: s.sReader.cr, m: &s.bufChks[i]}
		}
		s.curr = &storage.ChunkSeriesEntry{
			Lset: make(labels.Labels, len(s.bufLbls)),
			ChunkIteratorFn: func() chunks.Iterator {
				return storage.NewListChunkSeriesIterator(s.bufChks...)
			},
		}
		// TODO: Do we need to copy this?
		copy(s.curr.Lset, s.bufLbls)
		return true
	}
	return false
}

func (s *lazyPopulateChunkSeriesSet) At() storage.ChunkSeries {
	return s.curr
}

func (s *lazyPopulateChunkSeriesSet) Err() error {
	if s.err != nil {
		return s.err
	}
	return s.all.Err()
}

func (s *lazyPopulateChunkSeriesSet) Warnings() storage.Warnings { return nil }

// populatableChunk allows to trigger when you want to have chunks populated.
type populatableChunk interface {
	Populate(intervals tombstones.Intervals) (err error)
}

type lazyPopulatableChunk struct {
	m *chunks.Meta

	cr tsdb.ChunkReader

	populated chunkenc.Chunk
	bufIter   *tsdb.DeletedIterator
}

type errChunkIterator struct{ err error }

func (e errChunkIterator) Seek(int64) bool      { return false }
func (e errChunkIterator) At() (int64, float64) { return 0, 0 }
func (e errChunkIterator) Next() bool           { return false }
func (e errChunkIterator) Err() error           { return e.err }

var EmptyChunk = errChunk{err: errChunkIterator{err: errors.New("no samples")}}

type errChunk struct{ err errChunkIterator }

func (e errChunk) Bytes() []byte                                { return nil }
func (e errChunk) Encoding() chunkenc.Encoding                  { return chunkenc.EncXOR }
func (e errChunk) Appender() (chunkenc.Appender, error)         { return nil, e.err.err }
func (e errChunk) Iterator(chunkenc.Iterator) chunkenc.Iterator { return e.err }
func (e errChunk) NumSamples() int                              { return 0 }
func (e errChunk) Compact()                                     {}

func (l *lazyPopulatableChunk) Populate(intervals tombstones.Intervals) {
	if len(intervals) > 0 && (tombstones.Interval{Mint: l.m.MinTime, Maxt: l.m.MaxTime}.IsSubrange(intervals)) {
		l.m.Chunk = EmptyChunk
		return
	}

	// TODO(bwplotka): In most cases we don't need to parse anything, just copy. Extend reader/writer for this.
	var err error
	l.populated, err = l.cr.Chunk(l.m.Ref)
	if err != nil {
		l.m.Chunk = errChunk{err: errChunkIterator{err: errors.Wrapf(err, "cannot populate chunk %d", l.m.Ref)}}
		return
	}

	var matching tombstones.Intervals
	for _, interval := range intervals {
		if l.m.OverlapsClosedInterval(interval.Mint, interval.Maxt) {
			matching = matching.Add(interval)
		}
	}

	if len(matching) == 0 {
		l.m.Chunk = l.populated
		return
	}

	// TODO(bwplotka): Optimize by using passed iterator.
	l.bufIter = &tsdb.DeletedIterator{Intervals: matching, Iter: l.populated.Iterator(nil)}
	return

}

func (l *lazyPopulatableChunk) Bytes() []byte {
	if l.populated == nil {
		l.Populate(nil)
	}
	return l.populated.Bytes()
}

func (l *lazyPopulatableChunk) Encoding() chunkenc.Encoding {
	if l.populated == nil {
		l.Populate(nil)
	}
	return l.populated.Encoding()
}

func (l *lazyPopulatableChunk) Appender() (chunkenc.Appender, error) {
	if l.populated == nil {
		l.Populate(nil)
	}
	return l.populated.Appender()
}

func (l *lazyPopulatableChunk) Iterator(iterator chunkenc.Iterator) chunkenc.Iterator {
	if l.populated == nil {
		l.Populate(nil)
	}
	if l.bufIter == nil {
		return l.populated.Iterator(iterator)
	}
	return l.bufIter
}

func (l *lazyPopulatableChunk) NumSamples() int {
	if l.populated == nil {
		l.Populate(nil)
	}
	return l.populated.NumSamples()
}

func (l *lazyPopulatableChunk) Compact() {
	if l.populated == nil {
		l.Populate(nil)
	}
	l.populated.Compact()
}

func (w *seriesWriter) write(ctx context.Context, symbols index.StringIter, populatedSet storage.ChunkSeriesSet, sWriter SeriesWriter) error {
	var (
		chks []chunks.Meta
		ref  uint64
	)

	for symbols.Next() {
		if err := sWriter.AddSymbol(symbols.At()); err != nil {
			return errors.Wrap(err, "add symbol")
		}
	}
	if err := symbols.Err(); err != nil {
		return errors.Wrap(err, "symbols")
	}

	// Iterate over all sorted chunk series.
	for populatedSet.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		s := populatedSet.At()
		chksIter := s.Iterator()
		chks = chks[:0]
		for chksIter.Next() {
			// We are not iterating in streaming way over chunk as it's more efficient to do bulk write for index and
			// chunk file purposes.
			chks = append(chks, chksIter.At())
		}

		if chksIter.Err() != nil {
			return errors.Wrap(chksIter.Err(), "chunk iter")
		}

		// Skip the series with all deleted chunks.
		if len(chks) == 0 {
			continue
		}

		if err := sWriter.WriteChunks(chks...); err != nil {
			return errors.Wrap(err, "write chunks")
		}
		if err := sWriter.AddSeries(ref, s.Labels(), chks...); err != nil {
			return errors.Wrap(err, "add series")
		}

		for _, chk := range chks {
			if err := w.chunkPool.Put(chk.Chunk); err != nil {
				return errors.Wrap(err, "put chunk")
			}
		}
		ref++
	}
	if populatedSet.Err() != nil {
		return errors.Wrap(populatedSet.Err(), "iterate populated chunk series set")
	}

	return nil
}
