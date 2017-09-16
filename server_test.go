package nbd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

type readWriter struct {
	r io.ReadCloser
	w io.WriteCloser
}

type fuzzSizeWriter struct {
	io.WriteCloser
}

type testDriver []byte

type resp struct {
	reply
	payload []byte
}

type req struct {
	request
	payload  []byte
	respChan chan resp
}

type nbdClient struct {
	conn         io.ReadWriteCloser
	lock         sync.RWMutex
	reqMap       map[uint64]*req
	reqChan      chan *req
	reqBatchChan chan []*req
}

func (c *nbdClient) writeReq(w io.Writer, req *req, buf *buffer) error {
	req.Magic = NBD_REQUEST_MAGIC
	marshalNbdRequest(*buf, &req.request)
	_, err := w.Write(*buf)
	if err != nil {
		return err
	}
	if len(req.payload) > 0 {
		_, err = w.Write(req.payload)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *nbdClient) reqReader() {
	var seq uint64
	var buf buffer
	buf.allocate(NBD_REQUEST_SIZE)
	reqChan := c.reqChan
	reqBatchChan := c.reqBatchChan
L:
	for reqChan != nil || reqBatchChan != nil {
		select {
		case req, ok := <-reqChan:
			if !ok {
				reqChan = nil
				continue
			}
			req.Handle = seq
			seq++
			c.lock.Lock()
			c.reqMap[req.Handle] = req
			c.lock.Unlock()
			err := c.writeReq(c.conn, req, &buf)
			if err != nil {
				close(c.reqChan)
				req.respChan <- resp{
					reply: reply{
						Error: 1,
					},
				}
				break L
			}
		case batch, ok := <-reqBatchChan:
			if !ok {
				reqBatchChan = nil
				continue
			}

			var w bytes.Buffer
			c.lock.Lock()
			for _, req := range batch {
				req.Handle = seq
				seq++
				c.reqMap[req.Handle] = req
				c.writeReq(&w, req, &buf)
			}
			c.lock.Unlock()
			_, err := c.conn.Write(w.Bytes())
			if err != nil {
				for _, req := range batch {
					req.respChan <- resp{
						reply: reply{
							Error: 1,
						},
					}
				}
				break L
			}
		}
	}

	if reqChan != nil {
		close(reqChan)
	}

	if reqBatchChan != nil {
		close(reqBatchChan)
	}

}

func (c *nbdClient) replyReader() {
	buf := make([]byte, NBD_REPLY_SIZE)
	for {
		_, err := io.ReadFull(c.conn, buf)
		if err != nil {
			break
		}
		var resp resp
		unmarshalNbdReply(buf, &resp.reply)
		if resp.Magic != NBD_REPLY_MAGIC {
			panic("Invalid reply")
		}
		c.lock.Lock()
		req := c.reqMap[resp.Handle]
		if req != nil {
			delete(c.reqMap, resp.Handle)
		}
		c.lock.Unlock()
		if req == nil {
			panic("Unknown handle")
		}
		if req.Type == NBD_CMD_READ && resp.Error == 0 {
			resp.payload = make([]byte, req.Length)
			_, err := io.ReadFull(c.conn, resp.payload)
			if err != nil {
				break
			}
		}
		req.respChan <- resp
	}
	c.Close()
}

func (c *nbdClient) Request(request *request, payload []byte) (r *reply, p []byte, err error) {
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("Error: %v", x)
		}
	}()
	ch := make(chan resp)
	rq := req{
		request:  *request,
		payload:  payload,
		respChan: ch,
	}
	c.reqChan <- &rq
	repl := <-ch
	return &repl.reply, repl.payload, nil
}

func (c *nbdClient) SendRequest(req *req) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("Error: %v", x)
		}
	}()

	if req.respChan == nil {
		req.respChan = make(chan resp, 1)
	}

	c.reqChan <- req
	return
}

func (c *nbdClient) SendRequestBatch(batch ...*req) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = fmt.Errorf("Error: %v", x)
		}
	}()

	for _, req := range batch {
		if req.respChan == nil {
			req.respChan = make(chan resp, 1)
		}
	}

	c.reqBatchChan <- batch
	return
}

func (c *nbdClient) Close() {
	defer func() {
		recover()
	}()

	close(c.reqChan)
	c.conn.Close()
}

func NewTestClient(conn io.ReadWriteCloser) *nbdClient {
	c := &nbdClient{
		conn:         conn,
		reqChan:      make(chan *req),
		reqBatchChan: make(chan []*req),
		reqMap:       make(map[uint64]*req),
	}
	go c.reqReader()
	go c.replyReader()

	return c
}

func (d testDriver) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(d)) {
		return 0, io.EOF
	}
	n = copy(p, d[off:])
	if n < len(p) {
		err = io.EOF
	}
	return
}

func (d testDriver) WriteAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(d)) {
		return 0, io.EOF
	}
	n = copy(d[off:], p)
	if n < len(p) {
		err = io.EOF
	}
	return
}

func (d testDriver) Trim(off, length int64) error {
	return nil
}

func (d testDriver) Flush() error {
	return nil
}

func (d testDriver) Close() error {
	return nil
}

func (d testDriver) HasTrim() bool {
	return false
}

func (d testDriver) HasFlush() bool {
	return false
}

func (rw *readWriter) Read(p []byte) (int, error) {
	return rw.r.Read(p)
}

func (rw *readWriter) Write(p []byte) (int, error) {
	return rw.w.Write(p)
}

func (rw *readWriter) Close() error {
	err := rw.r.Close()
	err1 := rw.w.Close()
	if err != nil {
		return err
	}
	if err1 != nil {
		return err1
	}
	return nil
}

func marshalNbdRequest(buf []byte, req *request) {
	binary.BigEndian.PutUint32(buf, req.Magic)
	binary.BigEndian.PutUint32(buf[4:8], req.Type)
	binary.BigEndian.PutUint64(buf[8:16], req.Handle)
	binary.BigEndian.PutUint64(buf[16:24], req.From)
	binary.BigEndian.PutUint32(buf[24:28], req.Length)
}

func unmarshalNbdReply(buf []byte, reply *reply) {
	reply.Magic = binary.BigEndian.Uint32(buf)
	reply.Error = binary.BigEndian.Uint32(buf[4:8])
	reply.Handle = binary.BigEndian.Uint64(buf[8:16])
}

func TestNewNbdServerConn(t *testing.T) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	clientConn := readWriter{r: r1, w: w2}
	serverConn := readWriter{r: r2, w: w1}

	driver := testDriver(make([]byte, 1024*1024))

	conn := NewServerConn(&serverConn, driver)
	go conn.Serve()

	client := NewTestClient(&clientConn)

	req := request{
		Magic:  NBD_REQUEST_MAGIC,
		Type:   NBD_CMD_WRITE,
		Handle: 0,
		From:   1000,
		Length: 1000,
	}

	payload := make([]byte, 1000)

	resp, _, _ := client.Request(&req, payload)
	if resp.Error != 0 {
		t.Fatalf("Received error: %d", resp.Error)
	}

	client.Close()

}

func makeRandBuf(size int) []byte {
	buf := make([]byte, size)
	rand.Read(buf)
	return buf
}

func (w fuzzSizeWriter) Write(p []byte) (n int, err error) {
	chunks := rand.Intn(len(p)-1) + 1
	offsets := make([]int, chunks)
	for i := range offsets {
		offsets[i] = rand.Intn(len(p))
	}

	sort.Ints(offsets)
	lastOffset := 0
	for _, offset := range offsets {
		if offset == lastOffset {
			continue
		}
		nn, err1 := w.WriteCloser.Write(p[lastOffset:offset])
		n += nn
		if err1 != nil {
			err = err1
			return
		}
		lastOffset = offset
	}

	nn, err1 := w.WriteCloser.Write(p[lastOffset:])
	n += nn
	err = err1
	return
}

func (w fuzzSizeWriter) Close() error {
	return w.WriteCloser.Close()
}

func fuzzWriteRead(client *nbdClient, offset, length, minBatch, maxBatch int) error {
	if minBatch <= 0 {
		minBatch = 1
	}
	if maxBatch <= 0 || maxBatch > length {
		maxBatch = length
	}

	batchSize := rand.Intn(maxBatch-minBatch) + minBatch

	reqs := make([]*req, batchSize)
	zoneLen := length / batchSize
	randBuf := makeRandBuf(zoneLen * 2)

	zoneStart := 0
	for i := range reqs {
		off := rand.Intn(zoneLen)
		l := rand.Intn(zoneLen - off)
		rps := rand.Intn(zoneLen)
		req := &req{
			request: request{
				Type:   NBD_CMD_WRITE,
				From:   uint64(offset + zoneStart + off),
				Length: uint32(l),
			},
			payload: randBuf[rps : rps+l],
		}

		reqs[i] = req
		zoneStart += zoneLen
	}

	client.SendRequestBatch(reqs...)

	readReqs := make([]*req, batchSize)
	for i := range readReqs {
		readReqs[i] = &req{
			request: request{
				Type:   NBD_CMD_READ,
				From:   reqs[i].From,
				Length: reqs[i].Length,
			},
		}
	}

	for i := range reqs {
		resp := <-reqs[i].respChan
		if resp.Error != 0 {
			return fmt.Errorf("Write error at %d, len %d: %d", reqs[i].From, reqs[i].Length, resp.Error)
		}
	}

	client.SendRequestBatch(readReqs...)

	for i := range readReqs {
		resp := <-readReqs[i].respChan
		if resp.Error != 0 {
			return fmt.Errorf("Read error at %d, len %d: %d", reqs[i].From, reqs[i].Length, resp.Error)
		}
		if !bytes.Equal(resp.payload, reqs[i].payload) {
			return fmt.Errorf("Payloads don't match at %d, len %d", reqs[i].From, reqs[i].Length)
		}
	}

	return nil
}

func TestFuzzWriteRead(t *testing.T) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	clientConn := readWriter{r: r1, w: fuzzSizeWriter{w2}}
	serverConn := readWriter{r: r2, w: w1}

	SIZE := 1024 * 1024
	ZONES := 4

	driver := testDriver(make([]byte, SIZE))

	conn := NewServerConn(&serverConn, driver)
	conn.SetMaxProc(ZONES)
	go conn.Serve()

	client := NewTestClient(&clientConn)

	rand.Seed(time.Now().UnixNano())

	res := make(chan error, ZONES)

	zoneSize := SIZE / ZONES
	start := 0
	for i := 0; i < ZONES; i++ {
		go func(start int) {
			for i := 0; i < 100; i++ {
				err := fuzzWriteRead(client, start, zoneSize, 1, 128)
				if err != nil {
					res <- err
					return
				}
			}
			res <- nil
		}(start)

		start += zoneSize
	}

	for i := 0; i < ZONES; i++ {
		err := <-res
		if err != nil {
			t.Fatal(err)
		}
	}

	client.Close()
	time.Sleep(100 * time.Millisecond)
}
