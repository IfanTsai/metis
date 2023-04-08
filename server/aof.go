package server

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/IfanTsai/go-lib/utils/byteutils"
	"github.com/IfanTsai/metis/config"
	"github.com/IfanTsai/metis/database"
	"github.com/IfanTsai/metis/datastruct"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

const (
	AofRewriteTempFilePrefix  = "temp-rewriteaof-"
	AofRewriteItemsPerCommand = 64
)

// feedAppendOnlyFile is used to feed the AOF file with the command that was just executed.
func feedAppendOnlyFile(srv *Server, cmd *command, dbID int, args []string) {
	var aofStr string

	if srv.aofSelectDBID != dbID {
		srv.aofSelectDBID = dbID
		aofStr = catAppendOnlyGenericCommand([]string{"select", strconv.Itoa(dbID)})
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

	// accumulate the differences between the tmp db and the current one in a buffer
	// if a background aof rewriting is in progress.
	if srv.backgroundTaskTypeAtomic.Load() == uint32(TypeBackgroundTaskAOFRewrite) {
		srv.aofRewriteBuf.WriteString(aofStr)
	}
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
	nWritten, err := srv.aofFile.WriteString(aofStr)
	if err != nil {
		log.Fatalf("Failed to write to AOF file: %v", err)
	}

	srv.aofBuf.Reset()
	srv.aofCurrentSize += uint(nWritten)

	// fsync if needed
	aofFileSync(srv)
}

// aofFileSync syncs the AOF file if needed.
func aofFileSync(srv *Server) {
	now := time.Now()
	if srv.aofFsync == config.TypeAppendFsyncAlways ||
		(srv.aofFsync == config.TypeAppendFsyncEverySecond && now.Sub(srv.aofLastFsyncTime) > time.Second) {
		switch srv.aofFsync {
		case config.TypeAppendFsyncAlways:
			if err := srv.aofFile.Sync(); err != nil {
				log.Panicf("Failed to fsync the AOF file: %v", err)
			}
		case config.TypeAppendFsyncEverySecond:
			go func() {
				if err := srv.aofFile.Sync(); err != nil {
					log.Panicf("Failed to fsync the AOF file: %v", err)
				}
			}()
		}

		srv.aofLastFsyncTime = now
	}
}

// loadAppendOnlyFile replays the AOF file.
func loadAppendOnlyFile(srv *Server) {
	// readline is a helper function that reads a line from the AOF file.
	readLine := func(reader *bufio.Reader) (string, error) {
		buf, err := reader.ReadBytes('\n')
		if err != nil {
			return "", err
		}

		return byteutils.B2S(bytes.TrimRight(buf, "\r\n")), nil
	}

	aofFile, err := os.Open(srv.aofFilename)
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

// rewriteAppendOnlyFileBackground rewrites the AOF file in background.
func rewriteAppendOnlyFileBackground(srv *Server) {
	if !srv.backgroundTaskTypeAtomic.CompareAndSwap(uint32(TypeBackgroundTaskNone), uint32(TypeBackgroundTaskAOFRewrite)) {
		return
	}

	tmpDBs := make([]*database.Databse, len(srv.dbs))
	for i, db := range srv.dbs {
		tmpDBs[i] = db.DeepCopy()
	}

	go rewriteAppendOnlyFile(tmpDBs, srv)
}

func rewriteAppendOnlyFile(dbs []*database.Databse, srv *Server) {
	tmpFile, err := os.CreateTemp("", AofRewriteTempFilePrefix)
	if err != nil {
		log.Panicf("Failed to create temp file: %v", err)
	}
	defer tmpFile.Close()

	// dump db from memory to AOF temp file
	for _, db := range dbs {
		if db.Dict.Size() == 0 {
			continue
		}

		if _, err := tmpFile.WriteString(catAppendOnlyGenericCommand([]string{"select", strconv.Itoa(db.ID)})); err != nil {
			log.Panicf("Failed to write to AOF file: %v", err)
		}

		iter := datastruct.NewDictIterator(db.Dict)
		defer iter.Release()

		var err error
		for entry := iter.Next(); entry != nil; entry = iter.Next() {
			key := entry.Key.(string)
			switch value := entry.Value.(type) {
			case string:
				err = rewriteStringObject(tmpFile, key, value)
			case *datastruct.Quicklist:
				err = rewriteListObject(tmpFile, key, value)
			case *datastruct.Dict:
				err = rewriteHashObject(tmpFile, key, value)
			case *datastruct.Set:
				err = rewriteSetObject(tmpFile, key, value)
			case *datastruct.Zset:
				err = rewriteZsetObject(tmpFile, key, value)
			default:
				log.Panicf("Unknown object type: %T", value)
			}

			if err != nil {
				log.Panicf("Failed to write to AOF file: %v", err)
			}

			if entry := db.Expire.Find(key); entry != nil {
				when := strconv.FormatInt(entry.Value.(int64)/1000, 10)
				if _, err := tmpFile.WriteString(catAppendOnlyGenericCommand([]string{"expireat", key, when})); err != nil {
					log.Panicf("Failed to write to AOF file: %v", err)
				}
			}
		}
	}

	srv.aofRewriteDoneCh <- tmpFile.Name()
}

func rewriteStringObject(file *os.File, key string, value string) error {
	aofStr := catAppendOnlyGenericCommand([]string{"set", key, value})
	if _, err := file.WriteString(aofStr); err != nil {
		return errors.Wrapf(err, "failed to write string to AOF file, key: %s, value: %s", key, value)
	}

	return nil
}

func rewriteListObject(file *os.File, key string, value *datastruct.Quicklist) error {
	items := make([]string, 0, value.Len())

	iter := datastruct.NewQuicklistIterator(value)
	for element := iter.Next(); element != nil; element = iter.Next() {
		items = append(items, element.(string))
	}

	itemsChunks := lo.Chunk(items, AofRewriteItemsPerCommand)
	for _, chunk := range itemsChunks {
		aofStr := catAppendOnlyGenericCommand(append([]string{"lpush", key}, chunk...))
		if _, err := file.WriteString(aofStr); err != nil {
			return errors.Wrapf(err, "failed to write qucik list to AOF file, key: %s, value: %v", key, chunk)
		}
	}

	return nil
}

func rewriteHashObject(file *os.File, key string, value *datastruct.Dict) error {
	items := make([]string, 0, value.Size())

	iter := datastruct.NewDictIterator(value)
	defer iter.Release()

	for entry := iter.Next(); entry != nil; entry = iter.Next() {
		items = append(items, entry.Key.(string), entry.Value.(string))
	}

	itemsChunks := lo.Chunk(items, AofRewriteItemsPerCommand*2)
	for _, chunk := range itemsChunks {
		aofStr := catAppendOnlyGenericCommand(append([]string{"hset", key}, chunk...))
		if _, err := file.WriteString(aofStr); err != nil {
			return errors.Wrapf(err, "failed to write hash to AOF file, key: %s, value: %v", key, chunk)
		}
	}

	return nil
}

func rewriteSetObject(file *os.File, key string, value *datastruct.Set) error {
	items := lo.Map(value.Range(), func(item interface{}, _ int) string {
		return item.(string)
	})

	itemsChunks := lo.Chunk(items, AofRewriteItemsPerCommand)
	for _, chunk := range itemsChunks {
		aofStr := catAppendOnlyGenericCommand(append([]string{"sadd", key}, chunk...))
		if _, err := file.WriteString(aofStr); err != nil {
			return errors.Wrapf(err, "failed to write set to AOF file, key: %s, value: %v", key, chunk)
		}
	}

	return nil
}

func rewriteZsetObject(file *os.File, key string, value *datastruct.Zset) error {
	items := make([]string, 0, value.Size())

	for _, item := range value.RangeByRank(0, value.Size()-1, false) {
		items = append(items, item.Member, strconv.FormatFloat(item.Score, 'f', -1, 64))
	}

	itemsChunks := lo.Chunk(items, AofRewriteItemsPerCommand*2)
	for _, chunk := range itemsChunks {
		aofStr := catAppendOnlyGenericCommand(append([]string{"zadd", key}, chunk...))
		if _, err := file.WriteString(aofStr); err != nil {
			return errors.Wrapf(err, "failed to write zset to AOF file, key: %s, value: %v", key, chunk)
		}
	}

	return nil
}

// aofRewriteDoneCallback is called when AOF rewrite is done in server cron
func aofRewriteDoneCallback(srv *Server) {
	defer srv.backgroundTaskTypeAtomic.Store(uint32(TypeBackgroundTaskNone))

	if srv.aofFile != nil {
		srv.aofFile.Close()
	}

	tmpAofFilename := <-srv.aofRewriteDoneCh
	defer os.Remove(tmpAofFilename)

	// occurs error "invalid cross-device link" if use os.Rename
	if err := exec.Command("mv", tmpAofFilename, srv.aofFilename).Run(); err != nil {
		log.Panicf("failed to mv: %v", err)
	}

	aofFile, err := os.OpenFile(srv.aofFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Panicf("failed to open file: %v", err)
	}

	//  append AOF rewrite buffer to AOF file
	if srv.aofRewriteBuf.Len() > 0 {
		if _, err := aofFile.WriteString(srv.aofRewriteBuf.String()); err != nil {
			log.Panicf("failed to write to AOF file: %v", err)
		}
	}

	srv.aofRewriteBuf.Reset()

	fileInfo, err := aofFile.Stat()
	if err != nil {
		log.Panicf("failed to stat file: %v", err)
	}

	srv.aofCurrentSize = uint(fileInfo.Size())
	srv.aofRewriteBaseSize = srv.aofCurrentSize
	srv.aofFile = aofFile

	aofFileSync(srv)
}
