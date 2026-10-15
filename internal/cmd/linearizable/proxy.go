package main

import (
	"io"
	"net"
	"sync"
)

// A link between two nodes carries what each sends the other, or cuts
// them off: the connections reset, as by a node gone, or silent, as by a
// network that drops all, their bytes held until it carries again.
type state int

const (
	carrying state = iota
	reset
	silent
)

// link is the network between two nodes: a proxy for each, through which
// it connects to the other.
type link struct {
	mu        sync.Mutex
	changed   *sync.Cond
	state     state
	conns     map[net.Conn]bool
	listeners []net.Listener
}

func (l *link) init() {
	if l.changed == nil {
		l.changed = sync.NewCond(&l.mu)
		l.conns = map[net.Conn]bool{}
	}
}

// proxy listens for a node connecting through the link to the node at
// addr, and returns where.
func (l *link) proxy(addr string) (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	l.mu.Lock()
	l.init()
	l.listeners = append(l.listeners, ln)
	l.mu.Unlock()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go l.carry(conn, addr)
		}
	}()
	return ln.Addr().String(), nil
}

// carry connects conn to addr, and copies between them, as the link lets.
func (l *link) carry(conn net.Conn, addr string) {
	if !l.wait() {
		conn.Close()
		return
	}
	to, err := net.Dial("tcp", addr)
	if err != nil {
		conn.Close()
		return
	}
	if !l.add(conn, to) {
		return
	}
	go l.copy(to, conn)
	l.copy(conn, to)
}

// copy copies from src to dst while the link carries, holding what it
// read while it is silent.
func (l *link) copy(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if !l.wait() {
				break
			}
			if _, err := dst.Write(buf[:n]); err != nil {
				break
			}
		}
		if err != nil {
			if err == io.EOF {
				if c, ok := dst.(*net.TCPConn); ok {
					c.CloseWrite()
					return
				}
			}
			break
		}
	}
	src.Close()
	dst.Close()
}

// wait waits while the link is silent, and reports whether it carries.
func (l *link) wait() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.state == silent {
		l.changed.Wait()
	}
	return l.state == carrying
}

// add keeps the connections, to reset them; or closes them at once, if
// the link is cut meanwhile.
func (l *link) add(conns ...net.Conn) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state == reset {
		for _, c := range conns {
			c.Close()
		}
		return false
	}
	for _, c := range conns {
		l.conns[c] = true
	}
	return true
}

// set has the link carry, reset or be silent.
func (l *link) set(s state) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.init()
	l.state = s
	if s == reset {
		for c := range l.conns {
			c.Close()
		}
		clear(l.conns)
	}
	l.changed.Broadcast()
}

func (l *link) close() {
	l.set(reset)
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ln := range l.listeners {
		ln.Close()
	}
}
