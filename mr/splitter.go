package mr

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

// SplitInput splits input files into map tasks at line boundaries.
// When splitSize <= 0, DefaultSplitSize is used.
func SplitInput(files []string, splitSize int64) ([]Split, error) {
	if splitSize <= 0 {
		splitSize = DefaultSplitSize
	}

	var splits []Split
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			return nil, err
		}
		if info.Size() == 0 {
			splits = append(splits, Split{File: file, Offset: 0, Length: 0})
			continue
		}

		f, err := os.Open(file)
		if err != nil {
			return nil, err
		}

		offset := int64(0)
		for offset < info.Size() {
			length := splitSize
			if offset+length > info.Size() {
				length = info.Size() - offset
			}

			if offset+length < info.Size() {
				// Advance to the next line boundary. Very long records are rejected
				// instead of being silently split across map tasks.
				if _, err := f.Seek(offset+length, io.SeekStart); err != nil {
					f.Close()
					return nil, err
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
					f.Close()
					return nil, fmt.Errorf("line exceeds max split scan (%d bytes) near %s offset %d", DefaultMaxSplitScan, file, offset+length)
				}
				length += extra
				if offset+length > info.Size() {
					length = info.Size() - offset
				}
			}

			splits = append(splits, Split{
				File:   file,
				Offset: offset,
				Length: length,
			})
			offset += length
		}
		f.Close()
	}

	if len(splits) == 0 {
		return []Split{}, nil
	}
	return splits, nil
}
