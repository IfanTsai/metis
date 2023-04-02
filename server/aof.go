package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/IfanTsai/go-lib/utils/byteutils"
	"github.com/IfanTsai/metis/config"
	"github.com/pkg/errors"
)

// feedAppendOnlyFile is used to feed the AOF file with the command that was just executed.
func feedAppendOnlyFile(srv *Server, cmd *command, dbID int, args []string) {
	var aofStr string

	if srv.appendSelectDBID != dbID {
		srv.appendSelectDBID = dbID
		aofStr = fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%d\r\n%d\r\n", len(strconv.Itoa(dbID)), dbID)
	}

	switch cmd.name {
	case "expire":
		// translate EXPIRE to EXPIREAT
		aofStr += catAppendOnlyExpireCommand(args)
	case "setex":
		// translate SETEX to SET and EXPIREAT
		aofStr += catAppendOnlyGenericCommand([]string{"set", args[1], args[3]})
		aofStr += catAppendOnlyExpireCommand([]string{"expireat", args[1], args[2]})
	default:
		aofStr += catAppendOnlyGenericCommand(args)
	}

	// append to the AOF buffer. This will be flushed on disk just before of re-entering the event loop.
	srv.aofBuf.WriteString(aofStr)
}

// catAppendOnlyExpireCommand is used to create the string representation of a EXPIRE command.
// It is translated to EXPIREAT.
func catAppendOnlyExpireCommand(args []string) string {
	expireInt, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse expire time: %v", err)
	}

	when := time.Now().Unix() + expireInt

	args[0] = "expireat"
	args[2] = strconv.FormatInt(when, 10)

	return catAppendOnlyGenericCommand(args)
}

// catAppendOnlyGenericCommand is used to create the string representation of a command
func catAppendOnlyGenericCommand(args []string) string {
	var sb strings.Builder

	sb.WriteString("*")
	sb.WriteString(strconv.Itoa(len(args)))
	sb.WriteString("\r\n")

	for _, arg := range args {
		sb.WriteString("$")
		sb.WriteString(strconv.Itoa(len(arg)))
		sb.WriteString("\r\n")
		sb.WriteString(arg)
		sb.WriteString("\r\n")
	}

	return sb.String()
}

// flushAppendOnlyFile flushes the AOF buffer on disk.
func flushAppendOnlyFile(srv *Server) {
	if srv.aofBuf.Len() == 0 {
		return
	}

	aofStr := srv.aofBuf.String()
	if _, err := srv.appendFile.WriteString(aofStr); err != nil {
		log.Fatalf("Failed to write to AOF file: %v", err)
	}

	srv.aofBuf.Reset()

	// fsync if needed
	now := time.Now()
	if srv.appendFync == config.TypeAppendFsyncAlways ||
		(srv.appendFync == config.TypeAppendFsyncEverySecond && now.Sub(srv.lastFsyncTime) > time.Second) {
		if err := srv.appendFile.Sync(); err != nil {
			log.Printf("Failed to fsync the AOF file: %v", err)
		}

		srv.lastFsyncTime = now
	}
}

// loadAppendOnlyFile replays the AOF file.
func loadAppendOnlyFile(srv *Server) {
	aofFile, err := os.Open(srv.appendFilename)
	if err != nil {
		log.Fatalf("Failed to open AOF file: %v", err)
	}
	defer aofFile.Close()

	fakeClient := NewClient(srv, -1)
	defer fakeClient.free()

	reader := bufio.NewReaderSize(aofFile, MaxInlineSize)
	for {
		buf, err := readLine(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			log.Panicf("Failed to read AOF file: %v", err)
		}

		if buf[0] != '*' {
			log.Panicf("Invalid AOF file format: %v", err)
		}

		argc, err := strconv.Atoi(buf[1:])
		if err != nil {
			log.Panicf("Invalid AOF file format: %v", err)
		}

		args := make([]string, argc)
		for i := 0; i < argc; i++ {
			buf, err = readLine(reader)
			if err != nil {
				log.Panicf("Failed to read AOF file: %v", err)
			}

			if buf[0] != '$' {
				log.Panicf("Invalid AOF file format: %v", err)
			}

			argLen, err := strconv.Atoi(buf[1:])
			if err != nil {
				log.Panicf("Invalid AOF file format: %v", err)
			}

			arg := make([]byte, argLen)
			if _, err := reader.Read(arg); err != nil {
				log.Panicf("Failed to read AOF file: %v", err)
			}

			// discard CRLF
			if _, err := reader.Read(make([]byte, 2)); err != nil {
				log.Panicf("Failed to read AOF file: %v", err)
			}

			args[i] = byteutils.B2S(arg)
		}

		cmd := lookupCommand(strings.ToLower(args[0]))
		if cmd == nil {
			log.Panicf("Unknown command '%s' reading the append only file", args[0])
		}

		fakeClient.args = args
		_ = cmd.proc(fakeClient)
	}
}

// readline is a helper function that reads a line from the AOF file.
func readLine(reader *bufio.Reader) (string, error) {
	buf, err := reader.ReadBytes('\n')
	if err != nil {
		return "", err
	}

	return byteutils.B2S(bytes.TrimRight(buf, "\r\n")), nil
}
