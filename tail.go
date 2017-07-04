// Copyright (c) 2015 HPE Software Inc. All rights reserved.
// Copyright (c) 2013 ActiveState Software Inc. All rights reserved.

package tail

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	// "strings"
	"syscall"
	"time"
	// "strings"
	"sync"
	"sync/atomic"

	"github.com/fujiwara/shapeio"
	utils "github.com/nagae-memooff/goutils"
	"github.com/nagae-memooff/tail/util"
	"github.com/nagae-memooff/tail/watch"
	"gopkg.in/tomb.v1"
	"regexp"
)

func init() {
	go func() {
		for {
			time.Sleep(time.Second)
			mem_usage, err := utils.GetMemUsage()
			if err != nil {
				continue
			}

			atomic.StoreInt64(&Mem, int64(mem_usage))
		}
	}()
}

var (
	ErrStop  = errors.New("tail should now stop")
	Mem      int64
	MemLimit int64 = 200 * 1048576
)

type Line struct {
	Text string
	Err  error // Error from tail
}

// NewLine returns a Line with present time.
func NewLine(text string) *Line {
	return &Line{text, nil}
}

// SeekInfo represents arguments to `os.Seek`
type SeekInfo struct {
	Offset int64
	Whence int // os.SEEK_*
}

type logger interface {
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Fatalln(v ...interface{})
	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
	Panicln(v ...interface{})
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// Config is used to specify how a file must be tailed.
type Config struct {
	// File-specifc
	Location  *SeekInfo // Seek to this location before tailing
	ReOpen    bool      // Reopen recreated files (tail -F)
	MustExist bool      // Fail early if the file does not exist
	Poll      bool      // Poll for file changes instead of using inotify
	Pipe      bool      // Is a named pipe (mkfifo)

	// Generic IO
	ReadLimit          int
	Follow             bool  // Continue looking for new lines (tail -f)
	MaxLineSize        int64 // If non-zero, split longer lines into multiple lines
	LinesChannelLength int

	// Logger, when nil, is set to tail.DefaultLogger
	// To disable logging: set field to tail.DiscardingLogger
	Regex  string
	Logger logger
}

type Tail struct {
	Filename   string
	Lines      chan *Line
	line_bytes int64
	Config

	file        *os.File
	reader      *bufio.Reader
	limitreader *shapeio.Reader

	watcher watch.FileWatcher
	changes *watch.FileChanges

	tomb.Tomb // provides: Done, Kill, Dying

	lk     sync.Mutex
	offset int64
	inode  uint64

	readLine func() (string, error)
	regex    *regexp.Regexp

	pre_read string
}

var (
	// DefaultLogger is used when Config.Logger == nil
	DefaultLogger = log.New(os.Stderr, "", log.LstdFlags)
	// DiscardingLogger can be used to disable logging output
	DiscardingLogger = log.New(ioutil.Discard, "", 0)
)

// TailFile begins tailing the file. Output stream is made available
// via the `Tail.Lines` channel. To handle errors during tailing,
// invoke the `Wait` or `Err` method after finishing reading from the
// `Lines` channel.
func TailFile(filename string, config Config) (t *Tail, err error) {
	if config.ReOpen && !config.Follow {
		util.Fatal("cannot set ReOpen without Follow.")
	}

	t = &Tail{
		Filename: filename,
		Lines:    make(chan *Line, config.LinesChannelLength),
		Config:   config,
	}

	if config.Location != nil && config.Location.Whence == 0 {
		t.offset = config.Location.Offset
	}

	// when Logger was not specified in config, use default logger
	if t.Logger == nil {
		t.Logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	if t.Poll {
		t.watcher = watch.NewPollingFileWatcher(filename)
	} else {
		t.watcher = watch.NewInotifyFileWatcher(filename)
	}

	if t.MustExist {
		var err error
		t.file, err = OpenFile(t.Filename)
		if err != nil {
			return nil, err
		}
	}

	if config.Regex != "" {
		t.regex, err = regexp.Compile(config.Regex)
		if err != nil {
			t.regex = regexp.MustCompile("")
		}

		t.readLine = t._readXLine
	} else {
		t.readLine = t._readLine
	}

	go t.tailFileSync()

	return t, nil
}

func (tail *Tail) Offset() (offset int64) {
	return atomic.LoadInt64(&(tail.offset))
}

func (tail *Tail) AddLineBytes(value int64) {
	atomic.AddInt64(&tail.line_bytes, value)
}

// Stop stops the tailing activity.
func (tail *Tail) Stop() error {
	tail.Kill(nil)
	return tail.Wait()
}

// StopAtEOF stops tailing as soon as the end of the file is reached.
func (tail *Tail) StopAtEOF() error {
	tail.Kill(errStopAtEOF)
	return tail.Wait()
}

var errStopAtEOF = errors.New("tail: stop at eof")

func (tail *Tail) close() {
	close(tail.Lines)
	tail.closeFile()
}

func (tail *Tail) closeFile() {
	if tail.file != nil {
		tail.file.Close()
		tail.file = nil
	}
}

func (tail *Tail) reopen() error {
	tail.closeFile()
	for {
		var err error
		tail.file, err = OpenFile(tail.Filename)
		if err != nil {
			if os.IsNotExist(err) {
				tail.Logger.Printf("Waiting for %s to appear...", tail.Filename)
				if err := tail.watcher.BlockUntilExists(&tail.Tomb); err != nil {
					if err == tomb.ErrDying {
						return err
					}
					return fmt.Errorf("Failed to detect creation of %s: %s", tail.Filename, err)
				}
				continue
			}
			return fmt.Errorf("Unable to open file %s: %s", tail.Filename, err)
		}

		fi, _ := os.Stat(tail.Filename)
		stat, _ := fi.Sys().(*syscall.Stat_t)

		tail.inode = stat.Ino
		tail.watcher.SetInode(stat.Ino)
		break
	}
	return nil
}

func (tail *Tail) _readLine() (string, error) {
	tail.lk.Lock()
	line, err := tail.reader.ReadString('\n')
	tail.lk.Unlock()

	if err != nil {
		// Note ReadString "returns the data read before the error" in
		// case of an error, including EOF, so we return it as is. The
		// caller is expected to process it if err is EOF.
		return line, err
	}

	tail.offset = atomic.AddInt64(&(tail.offset), int64(len(line)))
	return line, err
}

func (tail *Tail) _readXLine() (line string, err error) {
	tail.lk.Lock()
	defer tail.lk.Unlock()

	if tail.pre_read == "" {
		// 若这一行不是正经日志， 就一直读，直到先读到正经的一行
		for !tail.regex.MatchString(tail.pre_read) {
			tail.pre_read, err = tail.reader.ReadString('\n')
			if err != nil {
				return line, err
			}
		}
	}

	nextline, err := tail.reader.ReadString('\n')
	if err != nil {
		return line, err
	}

	if tail.regex.MatchString(nextline) {
		line = tail.pre_read
		tail.pre_read = nextline

		tail.offset = atomic.AddInt64(&(tail.offset), int64(len(line)))
		return line, nil

	} else {
		// 若不包含，说明是异常日志，开启多行模式
		// 	mline := make([]string, 0, 16)
		// 	mline = append(mline, tail.pre_read, nextline)

		// 	for {
		// 		line, err := tail.reader.ReadString('\n')
		// 		if err != nil {
		// 			line = "wait"
		// 			return line, err
		// 		}

		// 		if !tail.regex.MatchString(line) {
		// 			mline = append(mline, line)
		// 		} else {
		// 			tail.pre_read = line
		// 			break
		// 		}
		// 	}
		// 	// 此时 mline里是一条数据， pre_read是下一条数据
		// 	line = strings.Join(mline, "")
		// 	mline = mline[:0]

		mbuffer := bytes.NewBuffer(make([]byte, 0, 4096))
		mbuffer.WriteString(tail.pre_read)
		mbuffer.WriteString(nextline)

		for {
			line, err := tail.reader.ReadBytes('\n')
			if err != nil {
				// fmt.Printf("pre read: '%s' \n", tail.pre_read)
				mbuffer.Write(line)
				tail.pre_read = string(mbuffer.Bytes())
				return "wait", err
			}

			line_str := string(line)

			if !tail.regex.MatchString(line_str) {
				mbuffer.Write(line)
			} else {
				copy_line := make([]byte, len(line))
				for i, b := range line {
					copy_line[i] = b
				}

				tail.pre_read = string(copy_line)
				break
			}
		}

		line = string(mbuffer.Bytes())
	}

	tail.offset = atomic.AddInt64(&(tail.offset), int64(len(line)))
	return line, err
}

func (tail *Tail) tailFileSync() {
	defer tail.Done()
	defer tail.close()

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	if !tail.MustExist {
		// deferred first open.
		err := tail.reopen()
		if err != nil {
			if err != tomb.ErrDying {
				tail.Kill(err)
			}
			return
		}
	}

	// Seek to requested location on first open of the file.
	if tail.Location != nil {
		var (
			file_size, offset int64
			err               error
		)

		if tail.Location.Whence == os.SEEK_END && tail.Location.Offset == 0 {
			// 说明从末尾开始
			offset, _ = tail.file.Seek(tail.Location.Offset, tail.Location.Whence)
			tail.offset = offset

		} else {
			file_size, _ = tail.file.Seek(0, os.SEEK_END)
			offset, err = tail.file.Seek(tail.Location.Offset, tail.Location.Whence)

			if offset > file_size {
				// 说明文件被悄悄地截断过，从末尾开始读即可
				tail.file.Seek(0, os.SEEK_END)
				tail.offset = file_size
			}
		}

		// tail.Logger.Printf("Seeked %s - %+v \n", tail.Filename, tail.Location)
		if err != nil {
			tail.Killf("Seek error on %s: %s", tail.Filename, err)
			return
		}
	}

	tail.openReader()

	// Read line by line.
	for {
		select {
		case <-ticker.C:
			// 检查
			will_reopen := false

			fi, err := os.Stat(tail.Filename)
			if os.IsNotExist(err) {
				will_reopen = true

			} else {
				stat, ok := fi.Sys().(*syscall.Stat_t)
				if (ok && stat.Ino != tail.inode) || fi.Size() < tail.offset {
					will_reopen = true
				}
			}

			if !will_reopen {
				continue
			}

			// HERE
			tail.changes = nil
			tail.Logger.Printf("Re-opening file %s ...", tail.Filename)
			if err := tail.reopen(); err != nil {
				continue
			}

			tail.Logger.Printf("Successfully reopened %s", tail.Filename)
			atomic.StoreInt64(&tail.offset, 0)
			tail.openReader()
			// tail.pre_read = ""

		default:
			line, err := tail.readLine()

			// Process `line` even if err is EOF.
			if err == nil {
				_ = tail.sendLine(line)
			} else if err == io.EOF {
				if !tail.Follow {
					if line != "" {
						tail.sendLine(line)
					}
					return
				}

				if tail.Follow && line != "" && tail.regex == nil {
					// this has the potential to never return the last line if
					// it's not followed by a newline; seems a fair trade here
					err := tail.seekTo(SeekInfo{Offset: tail.offset, Whence: 0})
					if err != nil {
						tail.Kill(err)
						return
					}
				} else if tail.Follow && line == "" {
					// TODO 说明被截断了？
				}

				// When EOF is reached, wait for more data to become
				// available. Wait strategy is based on the `tail.watcher`
				// implementation (inotify or polling).
				err := tail.waitForChanges()
				if err != nil {
					if err != ErrStop {
						tail.Kill(err)
					}
					return
				}
			} else {
				// non-EOF error
				tail.Killf("Error reading %s: %s", tail.Filename, err)
				return
			}

			select {
			case <-tail.Dying():
				if tail.Err() == errStopAtEOF {
					continue
				}
				return
			default:
				if WaitIfOutOfMemory() {
					continue
				}
			}
		}
	}
}

// waitForChanges waits until the file has been appended, deleted,
// moved or truncated. When moved or deleted - the file will be
// reopened if ReOpen is true. Truncated files are always reopened.
func (tail *Tail) waitForChanges() error {
	if tail.changes == nil {
		pos, err := tail.file.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}
		tail.changes, err = tail.watcher.ChangeEvents(&tail.Tomb, pos)
		if err != nil {
			return err
		}
	}

	select {
	case <-tail.changes.Modified:
		return nil
	case <-tail.changes.Deleted:
		tail.changes = nil
		if tail.ReOpen {
			// XXX: we must not log from a library.
			tail.Logger.Printf("Re-opening moved/deleted file %s ...", tail.Filename)
			if err := tail.reopen(); err != nil {
				return err
			}

			tail.Logger.Printf("Successfully reopened %s", tail.Filename)
			atomic.StoreInt64(&tail.offset, 0)
			tail.openReader()
			return nil
		} else {
			tail.Logger.Printf("Stopping tail as file no longer exists: %s", tail.Filename)
			return ErrStop
		}
	case <-tail.changes.Truncated:
		// Always reopen truncated files (Follow is true)
		tail.Logger.Printf("Re-opening truncated file %s ...", tail.Filename)
		atomic.StoreInt64(&tail.offset, 0)
		if err := tail.reopen(); err != nil {
			return err
		}
		tail.Logger.Printf("Successfully reopened truncated %s", tail.Filename)
		tail.openReader()
		return nil
	case <-tail.Dying():
		return ErrStop
	case <-time.After(time.Second):
		return nil
	}
	panic("unreachable")
}

func (tail *Tail) dropBrokenLine() (err error) {
	if tail.pre_read != "" {
		return
	}

	preread_bytes := 0
	defer func() {
		// fmt.Printf("preread: %d\n", preread_bytes)
		pos, _ := tail.file.Seek(0, os.SEEK_CUR)
		tail.offset = pos - int64(tail.reader.Buffered()) - int64(preread_bytes)

		// atomic.AddInt64(&tail.offset, int64(preread_bytes))
	}()

	if tail.regex != nil {
		// 若这一行不是正经日志， 就一直读，直到先读到正经的一行
		for !tail.regex.MatchString(tail.pre_read) {
			tail.pre_read, err = tail.reader.ReadString('\n')
			preread_bytes = len(tail.pre_read)

			if err != nil {
				return
			}
		}

	} else {
		if tail.Offset() > 0 {
			b := make([]byte, 1)
			_, err = tail.file.ReadAt(b, tail.offset-1)
			if err != nil {
				return
			}

			if b[0] != '\n' {
				line, err := tail.reader.ReadString('\n')
				preread_bytes = len(line)

				if err != nil {
					return err
				}

			}
		}
	}

	return
}

func (tail *Tail) openReader() {
	if tail.Config.ReadLimit > 0 {
		tail.limitreader = shapeio.NewReader(tail.file)
		tail.limitreader.SetRateLimit(float64(tail.Config.ReadLimit))

		tail.reader = bufio.NewReader(tail.limitreader)
	} else {
		tail.reader = bufio.NewReader(tail.file)
	}

	tail.dropBrokenLine()
}

func (tail *Tail) seekEnd() error {
	return tail.seekTo(SeekInfo{Offset: 0, Whence: os.SEEK_END})
}

func (tail *Tail) seekTo(pos SeekInfo) error {
	_, err := tail.file.Seek(pos.Offset, pos.Whence)
	if err != nil {
		return fmt.Errorf("Seek error on %s: %s", tail.Filename, err)
	}
	// Reset the read buffer whenever the file is re-seek'ed
	//   tail.offset = offset
	tail.reader.Reset(tail.file)
	return nil
}

// sendLine sends the line(s) to Lines channel, splitting longer lines
// if necessary. Return false if rate limit is reached.
func (tail *Tail) sendLine(line string) bool {
	// length := uint16(1)
	for tail.line_bytes > tail.Config.MaxLineSize {
		// fmt.Printf("wait: %d/%d\n", tail.line_bytes, tail.Config.MaxLineSize)
		time.Sleep(time.Millisecond * 100)
	}

	tail.Lines <- &Line{line, nil}
	tail.AddLineBytes(int64(len(line)))

	return true
}

// Cleanup removes inotify watches added by the tail package. This function is
// meant to be invoked from a process's exit handler. Linux kernel may not
// automatically remove inotify watches after the process exits.
func (tail *Tail) Cleanup() {
	watch.Cleanup(tail.Filename)
}

func WaitIfOutOfMemory() bool {
	_mem := atomic.LoadInt64(&Mem)
	if _mem > MemLimit {
		log.Printf("mem is overlimit: %d, wait a second.", _mem)
		time.Sleep(time.Second)
		return true
	}

	return false
}
