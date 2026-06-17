package mr

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
)

// PrepareSplits creates map input splits from job configuration.
// When nMap > 0, input is divided into exactly nMap line-aligned splits and
// splitSize is ignored. Otherwise splits are created every splitSize bytes;
// splitSize <= 0 uses DefaultSplitSize.
func PrepareSplits(files []string, splitSize int64, nMap int) ([]Split, error) {
	if nMap > 0 {
		return SplitInputByCount(files, nMap)
	}
	return SplitInput(files, splitSize)
}

// SplitInput splits input files into map tasks at line boundaries.
// When splitSize <= 0, DefaultSplitSize is used.
func SplitInput(files []string, splitSize int64) ([]Split, error) {
	if splitSize <= 0 {
		splitSize = DefaultSplitSize
	}

	var splits []Split
	for _, file := range files {
		parts, err := splitFileBySize(file, splitSize)
		if err != nil {
			return nil, err
		}
		splits = append(splits, parts...)
	}

	if len(splits) == 0 {
		return []Split{}, nil
	}
	return splits, nil
}

// SplitInputByCount divides input into exactly nMap line-aligned splits.
// Non-empty files receive at least one split; nMap must be >= the number of
// non-empty input files.
func SplitInputByCount(files []string, nMap int) ([]Split, error) {
	if nMap <= 0 {
		return nil, fmt.Errorf("nMap must be positive")
	}
	if len(files) == 0 {
		return []Split{}, nil
	}

	sizes := make([]int64, len(files))
	var total int64
	nonEmpty := 0
	for i, path := range files {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		sizes[i] = info.Size()
		total += sizes[i]
		if sizes[i] > 0 {
			nonEmpty++
		}
	}

	if total == 0 {
		splits := make([]Split, nMap)
		for i := range splits {
			splits[i] = Split{File: files[0], Offset: 0, Length: 0}
		}
		return splits, nil
	}
	if nMap < nonEmpty {
		return nil, fmt.Errorf("nMap (%d) must be >= number of non-empty input files (%d)", nMap, nonEmpty)
	}

	counts := distributeMapCounts(sizes, nMap)
	var splits []Split
	for i, path := range files {
		if counts[i] <= 0 {
			continue
		}
		parts, err := splitFileByCount(path, counts[i])
		if err != nil {
			return nil, err
		}
		splits = append(splits, parts...)
	}
	return splits, nil
}

func splitFileBySize(path string, splitSize int64) ([]Split, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return []Split{{File: path, Offset: 0, Length: 0}}, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var splits []Split
	offset := int64(0)
	for offset < info.Size() {
		length := splitSize
		if offset+length > info.Size() {
			length = info.Size() - offset
		}

		if offset+length < info.Size() {
			extra, err := extendSplitToLineBoundary(f, offset+length, info.Size())
			if err != nil {
				return nil, err
			}
			length += extra
			if offset+length > info.Size() {
				length = info.Size() - offset
			}
		}

		splits = append(splits, Split{
			File:   path,
			Offset: offset,
			Length: length,
		})
		offset += length
	}
	return splits, nil
}

func splitFileByCount(path string, nMap int) ([]Split, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return []Split{{File: path, Offset: 0, Length: 0}}, nil
	}
	if nMap <= 0 {
		return nil, fmt.Errorf("nMap must be positive")
	}
	if nMap == 1 {
		return []Split{{File: path, Offset: 0, Length: info.Size()}}, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	size := info.Size()
	target := size / int64(nMap)
	if target < 1 {
		target = 1
	}

	var splits []Split
	offset := int64(0)
	for i := 0; i < nMap; i++ {
		if offset >= size {
			splits = append(splits, Split{File: path, Offset: size, Length: 0})
			continue
		}

		length := target
		if i == nMap-1 {
			length = size - offset
		} else if offset+length < size {
			extra, err := extendSplitToLineBoundary(f, offset+length, size)
			if err != nil {
				return nil, err
			}
			length += extra
			if offset+length > size {
				length = size - offset
			}
		}

		splits = append(splits, Split{
			File:   path,
			Offset: offset,
			Length: length,
		})
		offset += length
	}
	return splits, nil
}

func extendSplitToLineBoundary(f *os.File, pos, fileSize int64) (int64, error) {
	if pos >= fileSize {
		return 0, nil
	}
	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		return 0, err
	}
	reader := bufio.NewReader(f)
	extra := int64(0)
	foundLine := false
	for {
		if extra >= DefaultMaxSplitScan {
			break
		}
		b, err := reader.ReadByte()
		if err != nil {
			break
		}
		extra++
		if b == '\n' {
			foundLine = true
			break
		}
	}
	if !foundLine && extra >= DefaultMaxSplitScan {
		utf8Extra, err := utf8BoundaryExtra(f, pos+extra, fileSize)
		if err != nil {
			return 0, err
		}
		extra += utf8Extra
	}
	return extra, nil
}

func distributeMapCounts(sizes []int64, nMap int) []int {
	counts := make([]int, len(sizes))
	var total int64
	nonEmpty := 0
	for _, s := range sizes {
		total += s
		if s > 0 {
			nonEmpty++
		}
	}
	if total == 0 || nMap <= 0 {
		return counts
	}

	type quota struct {
		idx    int
		base   int
		remain float64
	}
	quotas := make([]quota, 0, nonEmpty)
	assigned := 0
	for i, s := range sizes {
		if s == 0 {
			continue
		}
		exact := float64(nMap) * float64(s) / float64(total)
		base := int(exact)
		if base < 1 {
			base = 1
		}
		counts[i] = base
		assigned += base
		quotas = append(quotas, quota{idx: i, base: base, remain: exact - float64(base)})
	}

	for assigned > nMap {
		best := -1
		for _, q := range quotas {
			if counts[q.idx] > 1 && (best == -1 || counts[q.idx] > counts[best]) {
				best = q.idx
			}
		}
		if best == -1 {
			break
		}
		counts[best]--
		assigned--
	}

	sort.Slice(quotas, func(i, j int) bool {
		return quotas[i].remain > quotas[j].remain
	})
	for assigned < nMap {
		added := false
		for _, q := range quotas {
			if assigned >= nMap {
				break
			}
			counts[q.idx]++
			assigned++
			added = true
		}
		if !added {
			break
		}
	}
	return counts
}

func utf8BoundaryExtra(f *os.File, pos, size int64) (int64, error) {
	if pos >= size {
		return 0, nil
	}
	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		return 0, err
	}
	buf := make([]byte, 4)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return 0, err
	}
	extra := int64(0)
	for extra < int64(n) && isUTF8Continuation(buf[extra]) {
		extra++
	}
	return extra, nil
}

func isUTF8Continuation(b byte) bool {
	return b&0xC0 == 0x80
}
