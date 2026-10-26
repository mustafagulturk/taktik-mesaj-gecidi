// Command tmcli is a field-unit command line client for the TMG gateway.
//
//	tmcli listen -unit 20
//	tmcli send   -unit 10 -to 20 -type posrep -lat 39.92 -lon 32.85 -heading 270
//	tmcli send   -unit 10 -to all -type text -text "Tüm birlikler hazır olda" -prio flash
//	tmcli encode -type contact -lat 37.0 -lon 35.3 -category armor -count 4
//	tmcli decode c35a2240000a0014...
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mustafagulturk/taktik-mesaj-gecidi/client"
	"github.com/mustafagulturk/taktik-mesaj-gecidi/protocol"
)

const usage = `tmcli - TMG field unit client

Usage:
  tmcli listen [flags]          print frames addressed to this unit
  tmcli send   [flags]          send one message (acknowledged unless -noack)
  tmcli encode [flags]          print the wire encoding of a message as hex
  tmcli decode <hex>            decode a hex frame and show every field

Run "tmcli <command> -h" for the flags of a command.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "listen":
		err = cmdListen(os.Args[2:])
	case "send":
		err = cmdSend(os.Args[2:])
	case "encode":
		err = cmdEncode(os.Args[2:])
	case "decode":
		err = cmdDecode(os.Args[2:])
	case "-h", "--help", "help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type connFlags struct {
	addr    *string
	unit    *uint
	verbose *bool
}

func addConnFlags(fs *flag.FlagSet) connFlags {
	return connFlags{
		addr:    fs.String("addr", "127.0.0.1:7400", "gateway address"),
		unit:    fs.Uint("unit", 0, "this unit's address (1..65534), required"),
		verbose: fs.Bool("v", false, "log connection events"),
	}
}

func (c connFlags) client() (*client.Client, error) {
	lvl := slog.LevelError
	if *c.verbose {
		lvl = slog.LevelInfo
	}
	if *c.unit == 0 || *c.unit > 0xFFFE {
		return nil, errors.New("-unit must be in 1..65534")
	}
	return client.New(client.Config{
		Addr:   *c.addr,
		Unit:   uint16(*c.unit),
		Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})),
	})
}

func cmdListen(args []string) error {
	fs := flag.NewFlagSet("listen", flag.ExitOnError)
	cf := addConnFlags(fs)
	fs.Parse(args)
	c, err := cf.client()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go c.Run(ctx)
	for {
		select {
		case f := <-c.Incoming():
			fmt.Printf("%s %s\n", time.Now().Format("15:04:05.000"), describe(f))
		case <-ctx.Done():
			s := c.Stats()
			fmt.Fprintf(os.Stderr, "\nreceived=%d corrupt=%d skipped_bytes=%d reconnects=%d\n",
				s.FramesReceived, s.CorruptFrames, s.SkippedBytes, s.Connects-1)
			return nil
		}
	}
}

type msgFlags struct {
	typ, text, prio, class, category, order *string
	lat, lon, tlat, tlon                    *float64
	alt                                     *int
	speed, heading, count, confidence, at   *uint
	status                                  *uint
}

func addMsgFlags(fs *flag.FlagSet) msgFlags {
	return msgFlags{
		typ:        fs.String("type", "posrep", "posrep, contact, order or text"),
		text:       fs.String("text", "", "free text (max 63 characters, Turkish letters are folded)"),
		prio:       fs.String("prio", "routine", "routine, priority, immediate or flash"),
		class:      fs.String("class", "unclassified", "unclassified, restricted, confidential or secret"),
		category:   fs.String("category", "unknown", "contact category: unknown, infantry, armor, artillery, air, naval, vehicle, uav"),
		order:      fs.String("order", "hold", "order code: hold, advance, withdraw, regroup, observe, engage, cease_fire, resupply, medevac"),
		lat:        fs.Float64("lat", 0, "latitude in degrees"),
		lon:        fs.Float64("lon", 0, "longitude in degrees"),
		tlat:       fs.Float64("target-lat", 0, "order target latitude"),
		tlon:       fs.Float64("target-lon", 0, "order target longitude"),
		alt:        fs.Int("alt", 0, "altitude in metres"),
		speed:      fs.Uint("speed", 0, "speed in km/h (0..1023)"),
		heading:    fs.Uint("heading", 0, "heading in degrees (0..359)"),
		count:      fs.Uint("count", 1, "contact count (0..127)"),
		confidence: fs.Uint("confidence", 4, "contact confidence (0..7)"),
		at:         fs.Uint("at", 0, "order execution time as seconds after now"),
		status:     fs.Uint("status", 0, "unit status: 0 operational, 1 degraded, 2 engaged, 3 withdrawing, 4 out of action"),
	}
}

func lookup[T ~uint8](name, kind string, n int, str func(T) string) (T, error) {
	for i := 0; i < n; i++ {
		if strings.EqualFold(str(T(i)), name) {
			return T(i), nil
		}
	}
	return 0, fmt.Errorf("unknown %s %q", kind, name)
}

func secondsOfDay(t time.Time) uint32 {
	t = t.UTC()
	return uint32(t.Hour()*3600 + t.Minute()*60 + t.Second())
}

func (m msgFlags) build() (protocol.Message, client.Options, error) {
	var o client.Options
	var err error
	if o.Priority, err = lookup(*m.prio, "priority", 4, protocol.Priority.String); err != nil {
		return nil, o, err
	}
	if o.Classification, err = lookup(*m.class, "classification", 4, protocol.Classification.String); err != nil {
		return nil, o, err
	}
	now := secondsOfDay(time.Now())
	switch *m.typ {
	case "posrep":
		return &protocol.Position{
			Time: now, Pos: protocol.CoordFromDegrees(*m.lat, *m.lon), Alt: int16(*m.alt),
			Speed: uint16(*m.speed), Heading: uint16(*m.heading), Status: protocol.UnitStatus(*m.status),
		}, o, nil
	case "contact":
		cat, err := lookup(*m.category, "category", 8, protocol.ContactCategory.String)
		if err != nil {
			return nil, o, err
		}
		return &protocol.Contact{
			Time: now, Pos: protocol.CoordFromDegrees(*m.lat, *m.lon), Category: cat, Count: uint8(*m.count),
			Heading: uint16(*m.heading), Speed: uint16(*m.speed), Confidence: uint8(*m.confidence),
		}, o, nil
	case "order":
		code, err := lookup(*m.order, "order", 9, protocol.OrderCode.String)
		if err != nil {
			return nil, o, err
		}
		return &protocol.Order{
			Time: now, Code: code, Target: protocol.CoordFromDegrees(*m.tlat, *m.tlon),
			ExecuteAt: secondsOfDay(time.Now().Add(time.Duration(*m.at) * time.Second)),
		}, o, nil
	case "text":
		return protocol.NewFreeText(*m.text), o, nil
	}
	return nil, o, fmt.Errorf("unknown -type %q", *m.typ)
}

func parseDst(s string) (uint16, error) {
	if s == "all" || s == "broadcast" {
		return protocol.AddrBroadcast, nil
	}
	n, err := strconv.ParseUint(s, 0, 16)
	if err != nil || n == 0 || n == 0xFFFF {
		return 0, fmt.Errorf("-to must be 1..65534 or \"all\", got %q", s)
	}
	return uint16(n), nil
}

func cmdSend(args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	cf := addConnFlags(fs)
	mf := addMsgFlags(fs)
	to := fs.String("to", "all", "destination unit or \"all\"")
	noack := fs.Bool("noack", false, "fire and forget")
	timeout := fs.Duration("timeout", 15*time.Second, "give up after this long")
	fs.Parse(args)

	dst, err := parseDst(*to)
	if err != nil {
		return err
	}
	msg, opts, err := mf.build()
	if err != nil {
		return err
	}
	c, err := cf.client()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	go c.Run(ctx)

	start := time.Now()
	if *noack {
		if err := c.Post(ctx, dst, msg, opts); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond) // let the kernel flush before exit
		fmt.Println("sent")
		return nil
	}
	st, err := c.Send(ctx, dst, msg, opts)
	if err != nil {
		return err
	}
	fmt.Printf("%s in %v (retransmits=%d)\n", st, time.Since(start).Round(time.Millisecond), c.Stats().Retransmits)
	if st != protocol.AckDelivered && st != protocol.AckDuplicate {
		os.Exit(3)
	}
	return nil
}

func cmdEncode(args []string) error {
	fs := flag.NewFlagSet("encode", flag.ExitOnError)
	mf := addMsgFlags(fs)
	src := fs.Uint("unit", 1, "source unit")
	to := fs.String("to", "all", "destination unit or \"all\"")
	seq := fs.Uint("seq", 1, "sequence number")
	ack := fs.Bool("ack", false, "set the ack-required flag")
	fs.Parse(args)
	dst, err := parseDst(*to)
	if err != nil {
		return err
	}
	msg, o, err := mf.build()
	if err != nil {
		return err
	}
	f := &protocol.Frame{
		Header: protocol.Header{Priority: o.Priority, Classification: o.Classification, AckRequired: *ack,
			Src: uint16(*src), Dst: dst, Seq: uint16(*seq)},
		Msg: msg,
	}
	wire, err := protocol.Encode(f)
	if err != nil {
		return err
	}
	fmt.Println(hex.EncodeToString(wire))
	fmt.Fprintf(os.Stderr, "%d bytes: sync=2 header=%d payload=%d crc=2\n", len(wire), protocol.HeaderLen, len(wire)-protocol.MinFrameLen)
	return nil
}

func cmdDecode(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: tmcli decode <hex>")
	}
	b, err := hex.DecodeString(strings.ReplaceAll(args[0], " ", ""))
	if err != nil {
		return err
	}
	f, err := protocol.Decode(b)
	if err != nil {
		return err
	}
	fmt.Println(describe(f))
	return nil
}

func describe(f *protocol.Frame) string {
	dst := fmt.Sprintf("%d", f.Dst)
	if f.Dst == protocol.AddrBroadcast {
		dst = "ALL"
	}
	head := fmt.Sprintf("[%s %s] %d -> %s seq=%d", f.Priority, f.Classification, f.Src, dst, f.Seq)
	clock := func(s uint32) string { return fmt.Sprintf("%02d:%02d:%02dZ", s/3600, s/60%60, s%60) }
	switch m := f.Msg.(type) {
	case *protocol.Position:
		return fmt.Sprintf("%s POSREP %s pos=%s alt=%dm speed=%dkm/h hdg=%03d status=%s",
			head, clock(m.Time), m.Pos, m.Alt, m.Speed, m.Heading, m.Status)
	case *protocol.Contact:
		return fmt.Sprintf("%s CONTACT %s pos=%s %dx%s hdg=%03d speed=%dkm/h conf=%d/7",
			head, clock(m.Time), m.Pos, m.Count, m.Category, m.Heading, m.Speed, m.Confidence)
	case *protocol.Order:
		return fmt.Sprintf("%s ORDER %s %s target=%s execute=%s", head, clock(m.Time), m.Code, m.Target, clock(m.ExecuteAt))
	case *protocol.FreeText:
		return fmt.Sprintf("%s TEXT %q", head, m.Text)
	case *protocol.Heartbeat:
		return fmt.Sprintf("%s HEARTBEAT %s status=%s battery=%d%%", head, clock(m.Time), m.Status, m.Battery)
	case *protocol.Ack:
		return fmt.Sprintf("%s ACK seq=%d %s", head, m.AckedSeq, m.Status)
	}
	return head
}
