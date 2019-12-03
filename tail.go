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
	MemLimit int64 = 300 * 1048576
)

type Line struct {
	Order uint64
	Text  string
	Err   error // Error from tail
}

// NewLine returns a Line with present time.
func (tail *Tail) NewLine(text string) *Line {
	// TODO order
	return &Line{Text: text, Order: atomic.AddUint64(&tail.order, 1)}
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

	StartWithOrder uint64
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
	order    uint64
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

	t.order = config.StartWithOrder
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

func (tail *Tail) Order() (order uint64) {
	return atomic.LoadUint64(&tail.order)
}

func (tail *Tail) Offset() (offset int64) {
	return atomic.LoadInt64(&tail.offset)
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

// 不对日志进行正则匹配的情况，调用_readLine.否则，调用下面的_readXLine.
// 用read slice是由于有可能会出现异常情况，比如日志中存在异常的大量\0或超长行，导致一直读不到换行符，却把内存撑爆了
func (tail *Tail) _readLine() (string, error) {
	tail.lk.Lock()
	defer tail.lk.Unlock()

	sline, err := tail.reader.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		tail.Logger.Printf("bufio.ErrBufferFull in %s. May be the file is broken or the line is too long.", tail.Filename)
		tail.offset = atomic.AddInt64(&(tail.offset), int64(len(sline)))
		return "", err
	}

	line := CopyBytesToString(sline)

	if err != nil {
		// Note ReadString "returns the data read before the error" in
		// case of an error, including EOF, so we return it as is. The
		// caller is expected to process it if err is EOF.
		return line, err
	}

	tail.offset = atomic.AddInt64(&(tail.offset), int64(len(line)))
	return line, err
}

// 需要对日志进行正则匹配，以匹配多个物理行为一个逻辑行时，调用此方法。
// 用readslice的理由同上，主要是为了避免内存问题。此外，读多行的逻辑更加复杂，需要更小心。
func (tail *Tail) _readXLine() (line string, err error) {
	tail.lk.Lock()
	defer tail.lk.Unlock()
	var sline []byte

	// pre read 用于判断“上一行”是否是正常行。
	if tail.pre_read == "" {
		// 若这一行不是正常日志， 就一直读，直到先读到正经的一行，并丢弃不正常的行。
		for !tail.regex.MatchString(tail.pre_read) {
			sline, err = tail.reader.ReadSlice('\n')
			if err == bufio.ErrBufferFull {
				// 防止异常行或者超长行导致内存爆掉。
				tail.Logger.Printf("bufio.ErrBufferFull in %s. May be the file is broken or the line is too long.", tail.Filename)
				tail.offset = atomic.AddInt64(&(tail.offset), int64(len(sline)))
				continue
			}

			line := CopyBytesToString(sline)
			tail.pre_read = line
			if err != nil {
				return "", err
			}
		}
	}

	sline, err = tail.reader.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		tail.Logger.Printf("bufio.ErrBufferFull in %s. May be the file is broken or the line is too long.", tail.Filename)
		tail.offset = atomic.AddInt64(&(tail.offset), int64(len(sline)))
		return "", err
	}

	if err != nil && err != io.EOF {
		return "", err
	}

	var nextline string

	if err == io.EOF && len(sline) == 0 {
		return "", err
	} else if err == io.EOF && len(sline) > 0 && sline[len(sline)-1] != '\n' {
		nextline = CopyBytesToString(sline)

		// 说明没读完整行就eof了，需要继续读直到一整行出来
		// FIXME  是否有必要优化逻辑？ 目前这个代码有两个隐患：
		// 1、写如不完整的行之后，再也没有写完，会导致反复循环读取（比如磁盘满了等）
		// 2、超长行在这里无法被过滤丢弃
		var _nextline []byte

		for err == io.EOF {
			time.Sleep(10 * time.Millisecond)
			_nextline, err = tail.reader.ReadSlice('\n')
			nextline = nextline + string(_nextline)
		}

	} else {
		nextline = CopyBytesToString(sline)
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
		// fmt.Printf("pre read: '%s' \nnextline: '%s'\n", tail.pre_read, nextline)

		// FIXME 读完之前无法中断。考虑是否有办法做得好一点？
		for {
			line, err := tail.reader.ReadSlice('\n')
			// fmt.Printf("line: '%s', err: %s.\n", line , err)

			// fmt.Printf("pre read: '%s' \n", tail.pre_read)
			switch err {
			case bufio.ErrBufferFull:
				// bufio.ErrBufferFull说明buffer读满了也没读到换行符。此时要丢弃这一行，继续读
				tail.Logger.Printf("bufio.ErrBufferFull in %s. May be the file is broken or the line is too long.", tail.Filename)
				tail.offset = atomic.AddInt64(&(tail.offset), int64(len(line)))
				continue
			case io.EOF:
				if len(line) > 0 && line[len(line)-1] != '\n' {
					// io.EOF并且行尾不是回车，说明没等读到换行符就eof了，需要继续读取，直到读出来一个换行符为止
					var tail_line []byte
					xline := make([]byte, 0, len(line) * 2)
					xline = append(xline, line...)

					for err == io.EOF {
						time.Sleep(10 * time.Millisecond)
						tail_line, err = tail.reader.ReadSlice('\n')

						xline = append(xline, tail_line...)
					}

					line = xline
				} else {
					mbuffer.Write(line)
					tail.pre_read = string(mbuffer.Bytes())
					return "wait", err
				}
			case nil:
				// do nothing
			default:
				// 是否有可能发生其他错误?
				tail.Logger.Printf("read file error: %s. filename: %s.",err ,tail.Filename)
			}

			line_str := string(line)

			// 如果匹配不上正则，则说明要继续拼装。直到下一行匹配上了正则时，把之前的所有物理行视为同一个逻辑行，并结束循环匹配，保存pre_read.
			if !tail.regex.MatchString(line_str) {
				mbuffer.Write(line)
			} else {
				// copy_line := make([]byte, len(line))
				// for i, b := range line {
				// 	copy_line[i] = b
				// }

				tail.pre_read = CopyBytesToString(line)
				break
			}
		}

		line = string(mbuffer.Bytes())
	}

	tail.offset = atomic.AddInt64(&(tail.offset), int64(len(line)))
	return line, err
}

func (tail *Tail) tailFileSync() {

	ticker := time.NewTicker(time.Second * 10)
	defer func() {
		tail.Done()
		tail.close()
		ticker.Stop()
	}()

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
			} else if tail.Location.Offset < 0 && tail.Location.Whence == os.SEEK_SET {
				// 若为负数，说明是上一次logrotate后，未产生新的日志。这种情况下pre_read会被丢弃，且留下一个负数offset。
				// 此时需要将offset归零
				tail.file.Seek(0, os.SEEK_SET)
				tail.offset = 0
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
			// 每隔一定时间，检查inode是否发生变化，或者偏移量是否大于文件实际大小了。若有，则说明日志被截断或发生了logrotate
			will_reopen := false

			fi, err := os.Stat(tail.Filename)
			if os.IsNotExist(err) {
				will_reopen = true

			} else {
				stat, ok := fi.Sys().(*syscall.Stat_t)
				if (ok && stat.Ino != tail.inode) || fi.Size() < tail.Offset() {
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
			// atomic.StoreInt64(&tail.offset, 0)
			atomic.StoreInt64(&tail.offset, int64(-len(tail.pre_read)))
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
					err := tail.seekTo(SeekInfo{Offset: tail.Offset(), Whence: 0})
					if err != nil {
						tail.Kill(err)
						return
					}
				} else if tail.Follow && line == "" {
					// TODO 感觉好像不一定说明被截断了，也可能是preread的时候出错？
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
			} else if err == bufio.ErrBufferFull {
				tail.Logger.Printf("bufio.ErrBufferFull in %s. May be the file is broken or the line is too long.", tail.Filename)
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
				if tail.WaitIfOutOfMemory() {
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
			atomic.StoreInt64(&tail.offset, int64(-len(tail.pre_read)))

			tail.Logger.Printf("Successfully reopened %s", tail.Filename)
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
	case <-time.After(5 * time.Second):
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

	var sline []byte

	if tail.regex != nil {
		// 若这一行不是正经日志， 就一直读，直到先读到正经的一行
		for !tail.regex.MatchString(tail.pre_read) {
			sline, err = tail.reader.ReadSlice('\n')
			if err == bufio.ErrBufferFull {
				tail.Logger.Printf("bufio.ErrBufferFull in %s. May be the file is broken or the line is too long.", tail.Filename)
				continue
			}
			tail.pre_read = CopyBytesToString(sline)
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
				sline, err = tail.reader.ReadSlice('\n')
				if err == bufio.ErrBufferFull {
					tail.Logger.Printf("bufio.ErrBufferFull in %s. May be the file is broken or the line is too long.", tail.Filename)
					return
				}

				line := CopyBytesToString(sline)
				preread_bytes = len(line)

				if err != nil {
					return
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

		tail.reader = bufio.NewReaderSize(tail.limitreader, 1*1048576)
	} else {
		tail.reader = bufio.NewReaderSize(tail.file, 1*1048576)
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
	if line == "" {
		return false
	}

	for tail.line_bytes > tail.Config.MaxLineSize {
		// fmt.Printf("wait: %d/%d\nmsg:%s", tail.line_bytes, tail.Config.MaxLineSize,line)
		time.Sleep(time.Millisecond * 100)
	}

	tail.Lines <- tail.NewLine(line)
	tail.AddLineBytes(int64(len(line)))

	return true
}

// Cleanup removes inotify watches added by the tail package. This function is
// meant to be invoked from a process's exit handler. Linux kernel may not
// automatically remove inotify watches after the process exits.
func (tail *Tail) Cleanup() {
	watch.Cleanup(tail.Filename)
}

func (tail *Tail) WaitIfOutOfMemory() bool {
	_mem := atomic.LoadInt64(&Mem)
	if _mem > MemLimit {
		tail.Logger.Printf("mem is overlimit: %d, wait a second.", _mem)
		time.Sleep(time.Second)
		return true
	}

	return false
}

func CopyBytesToString(src []byte) (s string) {
	dst := make([]byte, len(src))
	copy(dst, src)

	s = string(dst)
	return
}
