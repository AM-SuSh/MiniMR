package mr

import (
	"bufio"
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
				// Advance to the next line boundary. If a line is extremely long,
				// stop scanning after DefaultMaxSplitScan and only extend far
				// enough to avoid cutting a UTF-8 rune in half.
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
					utf8Extra, err := utf8BoundaryExtra(f, offset+length+extra, info.Size())
					if err != nil {
						f.Close()
						return nil, err
					}
					extra += utf8Extra
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
