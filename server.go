package nbd

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"
)

const (
	NBD_CMD_READ  = 0
	NBD_CMD_WRITE = 1
	NBD_CMD_DISC  = 2
	NBD_CMD_FLUSH = 3
	NBD_CMD_TRIM  = 4
)

const (
	NBD_REQUEST_MAGIC = 0x25609513
	NBD_REPLY_MAGIC   = 0x67446698

	NBD_REQUEST_SIZE = 28
	NBD_REPLY_SIZE   = 16

	DEF_BUFFER_SIZE = 4096
)

// Driver is a minimal interface that an NBD driver must implement.
type Driver interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
}

// Trimmer is an optional interface that an NBD driver can implement. If it does, the server will handle the Trim command.
type Trimmer interface {
	Trim(off, length int64) error
}

// Syncer is an optional interface that an NBD driver can implement. If it does, the server will handle the Flush
// command using this interface.
type Syncer interface {
	Sync() error
}

// ServerConn is an instance of an NBD server connection.
type ServerConn struct {
	conn    io.ReadWriteCloser
	driver  Driver
	syncer  Syncer
	trimmer Trimmer
	pool    chan *cmdCtx

	writeLock sync.Mutex
	writeWg   sync.WaitGroup

	log *logrus.Logger
}

type ProcPool chan *cmdCtx

type handlerFunc func(c *ServerConn, ctx *cmdCtx) error

type cmdCtx struct {
	buf     buffer
	payload []byte
	request request
	reply   reply
}

type request struct {
	Magic  uint32
	Type   uint32
	Handle uint64
	From   uint64
	Length uint32
}

type reply struct {
	Magic  uint32
	Error  uint32
	Handle uint64
}

var cmdMap = [5]handlerFunc{
	opDeviceRead,
	opDeviceWrite,
	nil,
	opDeviceFlush,
	opDeviceTrim,
}

var ErrInvalidMagic = errors.New("received a packet with invalid Magic number")

// NewProcPool creates an instance of process pool with given size. It can be used to limit the number of
// simultaneously processed requests across multiple connections (see SetPool()).
func NewProcPool(size int) ProcPool {
	p := make(chan *cmdCtx, size)
	for i := 0; i < size; i++ {
		p <- newCtx()
	}

	return p
}

// NewServerConn creates a new server connection using an underlying conn.
// The connection must be fully negotiated.
// It can be any io.ReadWriteCloser, however when using a file handle (e.g. os.Pipe) it is highly
// recommended to wrap it into a net.Conn using net.FileConn(), otherwise performance will suffer.
// See https://groups.google.com/d/msg/golang-nuts/BMKB5pZc0co/CKqckjxA1gMJ for details.
func NewServerConn(conn io.ReadWriteCloser, driver Driver) *ServerConn {
	c := &ServerConn{
		conn:   conn,
		driver: driver,
	}

	c.trimmer, _ = driver.(Trimmer)
	c.syncer, _ = driver.(Syncer)

	return c
}

// SetMaxProc sets the maximum number of concurrently served requests. The default is 1.
func (c *ServerConn) SetMaxProc(p int) {
	c.pool = NewProcPool(p)
}

// SetPool sets the pool of command processing contexts allowing to use a shared pool across multiple connections.
func (c *ServerConn) SetPool(pool ProcPool) {
	c.pool = pool
}

// SetLogger sets the logger. If not set logrus.StandardLogger() is used.
func (c *ServerConn) SetLogger(log *logrus.Logger) {
	c.log = log
}

// Serve serves the connection until it's closed (either by the remote party or by calling Close()).
func (c *ServerConn) Serve() (err error) {
	if c.log == nil {
		c.log = logrus.StandardLogger()
	}

	if c.pool == nil {
		c.SetMaxProc(1)
	}

	var buf [NBD_REQUEST_SIZE]byte

	ctxTaken := false

	// Start handling requests
	for {
		ctxTaken = false

		_, err = io.ReadFull(c.conn, buf[:])
		if err != nil {
			break
		}

		ctx := <-c.pool
		ctxTaken = true
		unmarshalNbdRequest(buf[:], &ctx.request)
		c.log.Debugf("%#v", ctx.request)
		if ctx.request.Magic != NBD_REQUEST_MAGIC {
			err = ErrInvalidMagic
			break
		}

		tp := ctx.request.Type

		if tp < NBD_CMD_READ || tp > NBD_CMD_TRIM {
			go func() {
				c.log.Warnln("Received unknown request: ", ctx.request.Type)
				ctx.reply.Error = 22
				err := ctx.sendReply(c, ctx.buf.allocate(NBD_REPLY_SIZE))
				if err != nil {
					c.Close()
				}
			}()
		} else {
			if tp == NBD_CMD_WRITE {
				ctx.payload = ctx.buf.allocate(int(ctx.request.Length))
				_, err = io.ReadFull(c.conn, ctx.payload)
				if err != nil {
					break
				}
			}
			if tp == NBD_CMD_WRITE || tp == NBD_CMD_TRIM {
				c.writeWg.Add(1)
			} else if tp == NBD_CMD_FLUSH {
				c.writeWg.Wait()
			} else if tp == NBD_CMD_DISC {
				break
			}
			go func() {
				ctx.reply.Error = 0
				err := cmdMap[ctx.request.Type](c, ctx)
				if err != nil {
					c.log.Errorf("Request handler error: %v", err)
					c.Close()
				}
			}()
		}
	}

	n := cap(c.pool)
	if ctxTaken {
		n--
	}
	for i := 0; i < n; i++ {
		<-c.pool
	}
	c.conn.Close()
	c.driver.Close()

	return
}

// Close closes the connection interrupting the Serve().
func (c *ServerConn) Close() error {
	return c.conn.Close()
}

func (ctx *cmdCtx) sendReply(c *ServerConn, buf []byte) error {
	ctx.reply.Handle = ctx.request.Handle
	marshalNbdReply(buf, &ctx.reply)
	c.writeLock.Lock()
	_, err := c.conn.Write(buf)
	c.writeLock.Unlock()
	c.pool <- ctx
	return err
}

func newCtx() *cmdCtx {
	ctx := &cmdCtx{
		reply: reply{
			Magic: NBD_REPLY_MAGIC,
		},
	}
	ctx.buf.allocate(NBD_REPLY_SIZE + DEF_BUFFER_SIZE)
	return ctx
}

func unmarshalNbdRequest(buf []byte, request *request) {
	request.Magic = binary.BigEndian.Uint32(buf)
	request.Type = binary.BigEndian.Uint32(buf[4:8])
	request.Handle = binary.BigEndian.Uint64(buf[8:16])
	request.From = binary.BigEndian.Uint64(buf[16:24])
	request.Length = binary.BigEndian.Uint32(buf[24:28])
}

func marshalNbdReply(buf []byte, reply *reply) []byte {
	binary.BigEndian.PutUint32(buf[0:4], NBD_REPLY_MAGIC)
	binary.BigEndian.PutUint32(buf[4:8], reply.Error)
	binary.BigEndian.PutUint64(buf[8:16], reply.Handle)
	return buf
}

func nbdError(e error) uint32 {
	if os.IsPermission(e) {
		return 1
	}
	if serr, ok := e.(*os.SyscallError); ok {
		switch serr.Err {
		case syscall.EPERM:
			return 1
		case syscall.EIO:
			return 5
		case syscall.ENOMEM:
			return 12
		case syscall.ENOSPC, syscall.EFBIG, syscall.EDQUOT:
			return 28
		}
	}

	return 22
}

func opDeviceRead(c *ServerConn, ctx *cmdCtx) error {
	buf := ctx.buf.allocate(NBD_REPLY_SIZE + int(ctx.request.Length))
	if _, err := c.driver.ReadAt(buf[NBD_REPLY_SIZE:], int64(ctx.request.From)); err != nil {
		c.log.Errorln("driver.ReadAt returned an error:", err)
		ctx.reply.Error = nbdError(err)
		buf = buf[:NBD_REPLY_SIZE]
	}
	return ctx.sendReply(c, buf)
}

func opDeviceWrite(c *ServerConn, ctx *cmdCtx) error {
	if _, err := c.driver.WriteAt(ctx.payload, int64(ctx.request.From)); err != nil {
		c.log.Errorln("driver.WriteAt returned an error:", err)
		ctx.reply.Error = nbdError(err)
	}
	buf := ctx.buf.allocate(NBD_REPLY_SIZE)
	err := ctx.sendReply(c, buf)
	c.writeWg.Done()
	return err
}

func opDeviceFlush(c *ServerConn, ctx *cmdCtx) error {
	if c.syncer == nil {
		ctx.reply.Error = 22
	} else if err := c.syncer.Sync(); err != nil {
		c.log.Errorln("driver.Sync returned an error:", err)
		ctx.reply.Error = nbdError(err)
	}
	buf := ctx.buf.allocate(NBD_REPLY_SIZE)
	return ctx.sendReply(c, buf)
}

func opDeviceTrim(c *ServerConn, ctx *cmdCtx) error {
	if c.trimmer == nil {
		ctx.reply.Error = 22
	} else if err := c.trimmer.Trim(int64(ctx.request.From), int64(ctx.request.Length)); err != nil {
		c.log.Errorln("driver.Trim returned an error:", err)
		ctx.reply.Error = nbdError(err)
	}
	buf := ctx.buf.allocate(NBD_REPLY_SIZE)
	err := ctx.sendReply(c, buf)
	c.writeWg.Done()
	return err
}
