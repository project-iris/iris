// Iris - Decentralized cloud messaging
// Copyright (c) 2013 Project Iris. All rights reserved.
//
// Community license: for open source projects and services, Iris is free to use,
// redistribute and/or modify under the terms of the GNU Affero General Public
// License as published by the Free Software Foundation, either version 3, or (at
// your option) any later version.
//
// Evaluation license: you are free to privately evaluate Iris without adhering
// to either of the community or commercial licenses for as long as you like,
// however you are not permitted to publicly release any software or service
// built on top of it without a valid license.
//
// Commercial license: for commercial and/or closed source projects and services,
// the Iris cloud messaging system may be used in accordance with the terms and
// conditions contained in an individually negotiated signed written agreement
// between you and the author(s).

package iris

import (
	"crypto/rand"
	"encoding/gob"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"net"
	"sort"
	"sync"
	"time"

	"code.google.com/p/go.crypto/hkdf"
	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/proto"
	"github.com/project-iris/iris/proto/link"
	"github.com/project-iris/iris/proto/stream"
)

// The initialization packet when the tunnel is set up.
type initPacket struct {
	ConnId uint64 // Id of the Iris client connection requesting the tunnel
	TunId  uint64 // Id of the tunnel being built
}

// Authorization packet to send over the established encrypted tunnels.
type authPacket struct {
	Id uint64
}

// Header to attach to data transfer packets.
type dataHeader struct {
	SizeOrCont int // Size of the original message, or 0 if not the first chunk
}

// Make sure the handshake packets are registered with gob.
func init() {
	gob.Register(&initPacket{})
	gob.Register(&authPacket{})
	gob.Register(&dataHeader{})
}

func (o *Overlay) tunneler(ip net.IP, live chan struct{}, quit chan chan error) {
	// Listen for incoming streams on the given interface and random port.
	addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(ip.String(), "0"))
	if err != nil {
		panic(fmt.Sprintf("failed to resolve interface (%v): %v.", ip, err))
	}
	sock, err := stream.Listen(addr)
	if err != nil {
		panic(fmt.Sprintf("failed to start stream listener: %v.", err))
	}
	sock.Accept(config.IrisTunnelAcceptTimeout)

	// Save the new listener address into the local (sorted) address list
	o.lock.Lock()
	o.tunAddrs = append(o.tunAddrs, addr.String())
	sort.Strings(o.tunAddrs)
	o.lock.Unlock()

	// Notify the overlay of the successful listen
	live <- struct{}{}

	// Process incoming connection until termination is requested
	var errc chan error
	for errc == nil {
		select {
		case errc = <-quit:
			// Terminating, close and return
			continue
		case strm := <-sock.Sink:
			// There's a hidden panic possibility here: the listener socket can fail
			// if the system is overloaded with open connections. Alas, solving it is
			// not trivial as it would require restarting the whole listener. Figure it
			// out eventually.

			// Initialize and authorize the inbound tunnel
			if err := o.initServerTunnel(strm); err != nil {
				log.Printf("iris: failed to initialize server tunnel: %v.", err)
				if err := strm.Close(); err != nil {
					log.Printf("iris: failed to terminate uninitialized tunnel stream: %v.", err)
				}
			}
		}
	}
	// Terminate the peer listener
	errv := sock.Close()
	if errv != nil {
		log.Printf("iris: failed to terminate tunnel listener: %v.", err)
	}
	errc <- errv
}

// Communication stream between the local app and a remote endpoint. Ordered
// message delivery is guaranteed.
type Tunnel struct {
	id    uint64      // Auto-incremented tunnel identifier
	owner *Connection // Iris connection through which to communicate

	conn   *link.Link // Encrypted data link of the tunnel
	secret []byte     // Master key from which to derive the link keys

	initDone chan *link.Link // Channel to receive the reverse tunnel link
	initStop chan struct{}   // Channel to signal initialization abortion

	term chan struct{} // Channel to signal termination to blocked go-routines
	lock sync.Mutex    // Lock protecting the termination flag (init/close race)
}

// Initiates an outgoing tunnel to a remote cluster, by configuring a local
// tunnel endpoint and requesting the remote client to connect to it.
func (c *Connection) initiateTunnel(cluster string, timeout time.Duration) (*Tunnel, error) {
	// Create a potential tunnel
	c.tunLock.Lock()
	tunId := c.tunIdx
	tun := &Tunnel{
		id:    tunId,
		owner: c,

		initDone: make(chan *link.Link),
		initStop: make(chan struct{}),

		term: make(chan struct{}),
	}
	c.tunIdx++
	c.tunLive[tunId] = tun
	c.tunLock.Unlock()

	// Create the master encryption key
	tun.secret = make([]byte, config.StsCipherBits>>3)
	if _, err := io.ReadFull(rand.Reader, tun.secret); err != nil {
		return nil, err
	}
	// Send the tunneling request
	prefixIdx := int(tunId) % config.IrisClusterSplits
	c.iris.scribe.Balance(clusterPrefixes[prefixIdx]+cluster, c.assembleTunnelRequest(tunId, tun.secret, c.iris.tunAddrs, timeout))

	// Retrieve the results, time out or terminate
	var err error
	select {
	case <-c.term:
		close(tun.initStop)
		err = ErrTerminating
	case <-time.After(timeout):
		close(tun.initStop)
		err = ErrTimeout
	case conn := <-tun.initDone:
		// Make sure we haven't terminated in the mean while (init/close race)
		tun.lock.Lock()
		defer tun.lock.Unlock()

		select {
		case <-tun.term:
			err = ErrTerminating
			conn.Close()
			return nil, ErrTerminating
		default:
			// Finalize tunnel initiation and return
			tun.conn, tun.secret, tun.initDone, tun.initStop = conn, nil, nil, nil
			return tun, nil
		}
	}
	// Tunneling failed, clean up and report error
	c.tunLock.Lock()
	delete(c.tunLive, tunId)
	c.tunLock.Unlock()

	return nil, err
}

// Accepts an incoming tunneling request from a remote, initializes and stores
// the new tunnel into the connection state.
func (c *Connection) buildTunnel(remote uint64, id uint64, key []byte, addrs []string, timeout time.Duration) (*Tunnel, error) {
	deadline := time.Now().Add(timeout)

	// Create the local tunnel endpoint
	c.tunLock.Lock()
	tunId := c.tunIdx
	tun := &Tunnel{
		id:    tunId,
		owner: c,
		term:  make(chan struct{}),
	}
	c.tunIdx++
	c.tunLive[tunId] = tun
	c.tunLock.Unlock()

	// Dial the remote tunnel listener
	var err error
	var strm *stream.Stream
	for _, addr := range addrs {
		strm, err = stream.Dial(addr, timeout)
		if err == nil {
			break
		}
	}
	// If no error occurred, initialize the client endpoint
	if err == nil {
		var conn *link.Link
		conn, err = c.initClientTunnel(strm, remote, id, key, deadline)
		if err != nil {
			if err := strm.Close(); err != nil {
				log.Printf("iris: failed to close uninitialized client tunnel stream: %v.", err)
			}
		} else {
			// Make sure the tunnel wasn't terminated since (init/close race)
			tun.lock.Lock()
			select {
			case <-tun.term:
				conn.Close()
				err = ErrTerminating
			default:
				tun.conn = conn
			}
			tun.lock.Unlock()
		}
	}
	// Tunneling failed, clean up and report error
	if err != nil {
		c.tunLock.Lock()
		delete(c.tunLive, tunId)
		c.tunLock.Unlock()
		return nil, err
	}
	return tun, nil
}

// Initializes a stream into an encrypted tunnel link.
func (o *Overlay) initServerTunnel(strm *stream.Stream) error {
	// Set a socket deadline for finishing the handshake
	strm.Sock().SetDeadline(time.Now().Add(config.IrisTunnelInitTimeout))
	defer strm.Sock().SetDeadline(time.Time{})

	// Fetch the unencrypted client initiator
	init := new(initPacket)
	if err := strm.Recv(init); err != nil {
		return err
	}
	o.lock.RLock()
	c, ok := o.conns[init.ConnId]
	o.lock.RUnlock()
	if !ok {
		return errors.New("connection not found")
	}
	c.tunLock.RLock()
	tun, ok := c.tunLive[init.TunId]
	c.tunLock.RUnlock()
	if !ok {
		return errors.New("tunnel not found")
	}
	// Create the encrypted link
	hasher := func() hash.Hash { return config.HkdfHash.New() }
	hkdf := hkdf.New(hasher, tun.secret, config.HkdfSalt, config.HkdfInfo)
	conn := link.New(strm, hkdf, true)

	// Send and retrieve an authorization to verify both directions
	auth := &proto.Message{
		Head: proto.Header{
			Meta: &authPacket{Id: tun.id},
		},
	}
	if err := conn.SendDirect(auth); err != nil {
		return err
	}
	if msg, err := conn.RecvDirect(); err != nil {
		return err
	} else if auth, ok := msg.Head.Meta.(*authPacket); !ok || auth.Id != tun.id {
		return errors.New("protocol violation")
	}
	conn.Start(config.IrisTunnelBuffer)

	// Send back the initialized link to the pending tunnel
	select {
	case tun.initDone <- conn:
		// Connection handled by initiator
		return nil
	case <-tun.initStop:
		// Initiator timed out or terminated, close
		conn.Close()
		return nil // No error, since tunnel was handled, albeit not as expected
	}
}

// Initializes a stream into an encrypted tunnel link.
func (c *Connection) initClientTunnel(strm *stream.Stream, remote uint64, id uint64, key []byte, deadline time.Time) (*link.Link, error) {
	// Set a socket deadline for finishing the handshake
	strm.Sock().SetDeadline(deadline)
	defer strm.Sock().SetDeadline(time.Time{})

	// Send the unencrypted tunnel id to associate with the remote tunnel
	init := &initPacket{ConnId: remote, TunId: id}
	if err := strm.Send(init); err != nil {
		return nil, err
	}
	// Create the encrypted link and authorize it
	hasher := func() hash.Hash { return config.HkdfHash.New() }
	hkdf := hkdf.New(hasher, key, config.HkdfSalt, config.HkdfInfo)
	conn := link.New(strm, hkdf, false)

	// Send and retrieve an authorization to verify both directions
	auth := &proto.Message{
		Head: proto.Header{
			Meta: &authPacket{Id: id},
		},
	}
	if err := conn.SendDirect(auth); err != nil {
		return nil, err
	}
	if msg, err := conn.RecvDirect(); err != nil {
		return nil, err
	} else if auth, ok := msg.Head.Meta.(*authPacket); !ok || auth.Id != id {
		return nil, errors.New("protocol violation")
	}
	conn.Start(config.IrisTunnelBuffer)

	// Return the initialized link
	return conn, nil
}

// Closes the tunnel connection.
func (t *Tunnel) Close() error {
	if t.owner.handleTunnelClose(t.id) {
		// Synchronize between close and finishing init
		t.lock.Lock()
		defer t.lock.Unlock()
		close(t.term)

		// Handle race between close and init
		if t.conn != nil {
			return t.conn.Close()
		}
		return nil
	}
	return errors.New("tunnel already closed")
}

// Sends an asynchronous message to the remote pair. Not reentrant (order).
func (t *Tunnel) Send(size int, chunk []byte) error {
	// Create and encrypt the message
	packet := &proto.Message{
		Head: proto.Header{
			Meta: &dataHeader{size},
		},
		Data: chunk,
	}
	if err := packet.Encrypt(); err != nil {
		return err
	}
	// Queue the message for sending
	select {
	case t.conn.Send <- packet:
		return nil
	case <-t.term:
		return errors.New("closed")
	}
}

// Retrieves a message waiting in the local queue. If none is available, the
// call blocks until either one arrives or a timeout is reached.
func (t *Tunnel) Recv(timeout time.Duration) (int, []byte, error) {
	// Retrieve an encrypted packet from the tunnel link
	select {
	case packet, ok := <-t.conn.Recv:
		// Terminate the tunnel if closed remotely
		if !ok {
			t.Close()
			return 0, nil, ErrTerminating
		}
		// Decrypt and pass upstream
		if err := packet.Decrypt(); err != nil {
			return 0, nil, err
		}
		return packet.Head.Meta.(*dataHeader).SizeOrCont, packet.Data, nil

	case <-time.After(timeout):
		return 0, nil, ErrTimeout
	}
}
