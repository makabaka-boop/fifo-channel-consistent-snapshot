package main

// Transfer is an application message moving tokens between two nodes.
// ID is a unique correlation number assigned by the coordinator so every
// in-flight transfer in a snapshot can be traced back individually.
type Transfer struct {
	ID     string
	From   int
	To     int
	Amount int64
}

// Marker is a Chandy–Lamport marker. SnapID scopes it to exactly one
// snapshot so records of different snapshots can never be mixed.
type Marker struct {
	SnapID int
	From   int
	To     int
}

// Message is anything that travels over a directed channel.
type Message interface{ isMessage() }

func (Transfer) isMessage() {}
func (Marker) isMessage()   {}
