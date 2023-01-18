//go:build linux
// +build linux

package nbd

import (
	"os"
	"syscall"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	IO = 0xab << 8

	NBD_SET_SOCK        = IO
	NBD_SET_BLKSIZE     = IO | 1
	NBD_SET_SIZE        = IO | 2
	NBD_DO_IT           = IO | 3
	NBD_CLEAR_SOCK      = IO | 4
	NBD_CLEAR_QUE       = IO | 5
	NBD_PRINT_DEBUG     = IO | 6
	NBD_SET_SIZE_BLOCKS = IO | 7
	NBD_DISCONNECT      = IO | 8
	NBD_SET_TIMEOUT     = IO | 9
	NBD_SET_FLAGS       = IO | 10
)

const (
	NBD_FLAG_HAS_FLAGS  = (1 << 0)
	NBD_FLAG_READ_ONLY  = (1 << 1)
	NBD_FLAG_SEND_FLUSH = (1 << 2)
	NBD_FLAG_SEND_TRIM  = (1 << 5)
)

type Client struct {
	devpath   string
	devsize   int64
	blocksize int
	sockFd    int

	sendFlush, sendTrim bool
	readOnly            bool

	dev *os.File

	log *logrus.Logger
}

func ioctl(fd, op, arg uintptr) error {
	_, _, err := syscall.Syscall(syscall.SYS_IOCTL, fd, op, arg)
	if err != 0 {
		return errors.Wrapf(err, "ioctl(%d, %d, %d) failed", fd, op, arg)
	}

	return nil
}

func NewClient(devpath string, sockFd int, devsize int64) *Client {
	return &Client{
		devpath: devpath,
		sockFd:  sockFd,
		devsize: devsize,
	}
}

func (c *Client) SetSendFlush(v bool) {
	c.sendFlush = v
}

func (c *Client) SetSendTrim(v bool) {
	c.sendTrim = v
}

func (c *Client) SetBlockSize(size int) {
	c.blocksize = size
}

func (c *Client) SetReadOnly(flag bool) {
	c.readOnly = flag
}

func (c *Client) SetLogger(log *logrus.Logger) {
	c.log = log
}

func (c *Client) Run() (err error) {
	c.dev, err = os.OpenFile(c.devpath, os.O_RDWR, 0600)
	if err != nil {
		return
	}

	devFd := c.dev.Fd()

	if err = ioctl(devFd, NBD_SET_SIZE, uintptr(c.devsize)); err != nil {
		return
	}
	if err = ioctl(devFd, NBD_CLEAR_QUE, 0); err != nil {
		return
	}
	if err = ioctl(devFd, NBD_CLEAR_SOCK, 0); err != nil {
		return
	}
	if c.blocksize > 0 {
		if err = ioctl(devFd, NBD_SET_BLKSIZE, uintptr(c.blocksize)); err != nil {
			return
		}
	}

	var flags uintptr

	if c.sendFlush {
		flags |= NBD_FLAG_SEND_FLUSH
	}

	if c.sendTrim {
		flags |= NBD_FLAG_SEND_TRIM
	}

	if c.readOnly {
		flags |= NBD_FLAG_READ_ONLY
	}

	if err1 := ioctl(devFd, NBD_SET_FLAGS, flags); err1 != nil {
		// ignore errors as some features may not be supported
		if c.log != nil {
			c.log.Warnf("Could not set flags: %v", err1)
		}
	}

	if err = ioctl(devFd, NBD_SET_SOCK, uintptr(c.sockFd)); err != nil {
		return
	}

	go func() {
		//opens the device file at least once, to make sure the partition table is updated
		tmp, err := os.Open(c.devpath)
		if err != nil {
			if c.log != nil {
				c.log.Errorf("Cannot open the device %s: %s", c.devpath, err)
			}
		}
		tmp.Close()
	}()

	// The following call will block until the client disconnects
	err = ioctl(devFd, NBD_DO_IT, 0)
	if c.log != nil {
		c.log.Debug("Finished NBD client")
	}
	ioctl(devFd, NBD_CLEAR_SOCK, 0)
	c.dev.Close()
	syscall.Close(int(c.sockFd))
	return err
}

func (c *Client) Close() error {
	return ioctl(c.dev.Fd(), NBD_DISCONNECT, 0)
}
