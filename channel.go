package main

import "sync"

// Channel is a reliable FIFO directed link from one node to another.
//
// Enqueue never blocks the sender's message loop; a dedicated pump goroutine
// delivers messages in order to the receiver's inbox. A test barrier
// (Hold/Release) can pause delivery without dropping or reordering anything,
// which lets tests and operators control the exact delivery order — e.g. to
// keep a transfer "debited but not yet received" while a snapshot runs.
type Channel struct {
	From int
	To   int

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []Message
	held   bool
	closed bool

	stop chan struct{}
	done chan struct{}
}

func newChannel(from, to int, inbox chan<- Message) *Channel {
	c := &Channel{From: from, To: to, stop: make(chan struct{}), done: make(chan struct{})}
	c.cond = sync.NewCond(&c.mu)
	go c.pump(inbox)
	return c
}

// Enqueue appends a message to the reliable FIFO queue. It is wait-free, so a
// node can perform "debit + enqueue" as one atomic step inside its own loop.
func (c *Channel) Enqueue(m Message) {
	c.mu.Lock()
	c.queue = append(c.queue, m)
	c.cond.Signal()
	c.mu.Unlock()
}

func (c *Channel) pump(inbox chan<- Message) {
	defer close(c.done)
	for {
		c.mu.Lock()
		for len(c.queue) == 0 || c.held {
			if c.closed {
				c.mu.Unlock()
				return
			}
			c.cond.Wait()
		}
		m := c.queue[0]
		c.queue = c.queue[1:]
		c.mu.Unlock()

		select {
		case inbox <- m:
		case <-c.stop:
			return
		}
	}
}

// Hold engages the test barrier: queued and future messages stay in the
// queue (FIFO order preserved) but nothing is delivered.
func (c *Channel) Hold() {
	c.mu.Lock()
	c.held = true
	c.mu.Unlock()
}

// Release lifts the test barrier; delivery resumes in FIFO order.
func (c *Channel) Release() {
	c.mu.Lock()
	c.held = false
	c.cond.Signal()
	c.mu.Unlock()
}

// Len reports how many messages are currently queued (diagnostics/tests).
func (c *Channel) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.queue)
}

// Held reports whether the test barrier is engaged.
func (c *Channel) Held() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.held
}

func (c *Channel) close() {
	c.mu.Lock()
	c.closed = true
	c.cond.Broadcast()
	c.mu.Unlock()
	close(c.stop)
	<-c.done
}
