// Copyright (c) 2015 HPE Software Inc. All rights reserved.
// Copyright (c) 2013 ActiveState Software Inc. All rights reserved.

package watch

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/nagae-memooff/tail/util"

	"gopkg.in/fsnotify.v1"
	//   "github.com/fsnotify/fsnotify"
	"gopkg.in/tomb.v1"
)

// InotifyFileWatcher uses inotify to monitor file changes.
type InotifyFileWatcher struct {
	Filename string
	Size     int64
}

func NewInotifyFileWatcher(filename string) *InotifyFileWatcher {
	fw := &InotifyFileWatcher{filepath.Clean(filename), 0}
	return fw
}

func (fw *InotifyFileWatcher) SetInode(inode uint64) error {
	once.Do(goRun)

	shared.mux.Lock()
	defer shared.mux.Unlock()

	shared.inodes[fw.Filename] = inode
	return nil
}

func (fw *InotifyFileWatcher) BlockUntilExists(t *tomb.Tomb) error {
	err := WatchCreate(fw.Filename)
	if err != nil {
		return err
	}
	defer RemoveWatchCreate(fw.Filename)

	// Do a real check now as the file might have been created before
	// calling `WatchFlags` above.
	if _, err = os.Stat(fw.Filename); !os.IsNotExist(err) {
		// file exists, or stat returned an error.
		logger.Printf("err: %v", err)
		return err
	}

	events, _ := Events(fw.Filename)

	for {
		select {
		case evt, ok := <-events:

			if !ok {
				return fmt.Errorf("inotify watcher has been closed")
			}
			evtName, err := filepath.Abs(evt.Name)
			if err != nil {
				return err
			}
			fwFilename, err := filepath.Abs(fw.Filename)
			if err != nil {
				return err
			}
			if evtName == fwFilename {
				fi, err := os.Stat(fwFilename)
				if err == nil {
					stat, ok := fi.Sys().(*syscall.Stat_t)
					if ok {
						shared.inodes[fw.Filename] = stat.Ino
					}
				}
				return nil
			}

		case <-t.Dying():
			return tomb.ErrDying
		}
	}
	panic("unreachable")
}

func (fw *InotifyFileWatcher) ChangeEvents(t *tomb.Tomb, pos int64) (*FileChanges, error) {
	err := Watch(fw.Filename)
	if err != nil {
		return nil, err
	}

	changes := NewFileChanges()
	fw.Size = pos

	go func() {
		events, inode := Events(fw.Filename)

		for {
			prevSize := fw.Size

			var evt fsnotify.Event
			var ok bool

			select {
			case evt, ok = <-events:
				if !ok {
					RemoveWatch(fw.Filename)
					return
				}
			case <-t.Dying():
				RemoveWatch(fw.Filename)
				return
			}

			switch {
			//With an open fd, unlink(fd) - inotify returns IN_ATTRIB (==fsnotify.Chmod)
			case evt.Op&fsnotify.Chmod == fsnotify.Chmod:
				// i, err := os.Stat(fw.Filename)
				// if  err != nil {
				// 	if !os.IsNotExist(err) {
				// 		return
				// 	}
				// }
				// fallthrough

				// _, err := os.Stat(fw.Filename)
				// if err == nil {
				// 	continue
				// } else {
				// 	if !os.IsNotExist(err) {
				// 		logger.Printf("stat %s error: %s", fw.Filename, err)
				// 		return
				// 	}
				// }
				fallthrough

			case evt.Op&fsnotify.Remove == fsnotify.Remove:
				fallthrough

			case evt.Op&fsnotify.Rename == fsnotify.Rename:
				fi, err := os.Stat(fw.Filename)
				if os.IsNotExist(err) {
					RemoveWatch(fw.Filename)
					changes.NotifyDeleted()
					return
				} else {
					stat, ok := fi.Sys().(*syscall.Stat_t)
					if ok && stat.Ino != inode {
						RemoveWatch(fw.Filename)
						changes.NotifyDeleted()
						return
					}
				}

			case evt.Op&fsnotify.Write == fsnotify.Write:
				// TODO 精简这里的系统调用
				// changes.NotifyModified()
				fi, err := os.Stat(fw.Filename)
				if err != nil {
					if os.IsNotExist(err) {
						RemoveWatch(fw.Filename)
						changes.NotifyDeleted()
						return
					}
					// XXX: report this error back to the user
					util.Fatal("Failed to stat file %v: %v", fw.Filename, err)
				}
				fw.Size = fi.Size()

				if prevSize > 0 && prevSize > fw.Size {
					changes.NotifyTruncated()
				} else {
					changes.NotifyModified()
				}
				prevSize = fw.Size
			}
		}
	}()

	return changes, nil
}
