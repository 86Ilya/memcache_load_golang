package main

import (
	"appsinstalled"
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type options struct {
	idfa, gaid, adid, dvid, pattern, logfile string
	threads                                  int
}

var default_opt = options{
	idfa:    "127.0.0.1:33013",
	gaid:    "127.0.0.1:33014",
	adid:    "127.0.0.1:33015",
	dvid:    "127.0.0.1:33016",
	pattern: "data/appsinstalled/*.tsv.gz",
	logfile: "",
	threads: 2,
}

var logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

func parse_appinstalled(line string) (*appsinstalled.UserApps, string, string) {

	result := strings.Split(line, "\t")
	dev_type, dev_id, lat, lon, raw_apps := result[0], result[1], result[2], result[3], strings.Split(result[4], ",")
	n_apps := len(raw_apps)
	apps := make([]uint32, n_apps)

	for i, oneapp := range raw_apps {
		oneapp, _ := strconv.ParseInt(oneapp, 10, 32)
		apps[i] = uint32(oneapp)
	}
	Lat, _ := strconv.ParseFloat(lat, 64)
	Lon, _ := strconv.ParseFloat(lon, 64)

	app := &appsinstalled.UserApps{
		Apps: apps,
		Lat:  proto.Float64(Lat),
		Lon:  proto.Float64(Lon),
	}

	key := dev_type + ":" + dev_id

	return app, key, dev_type
}

func process_file(filename string, memc_clients map[string]*memcache.Client) {
	var errors_count, apps_count uint32 = 0, 0
	handle, err := os.Open(filename)
	if err != nil {
		logger.Println("File Open:", filename, err)
	}
	defer handle.Close()

	zipReader, err := gzip.NewReader(handle)
	if err != nil {
		logger.Println("New gzip reader:", err)
	}
	defer zipReader.Close()

	scanner := bufio.NewScanner(zipReader)
	scanner.Split(bufio.ScanLines)
	logger.Println("Processing file:", filename)
	for scanner.Scan() {
		app, key, dev_type := parse_appinstalled(scanner.Text())
		apps_count++
		if mc, ok := memc_clients[dev_type]; ok {
			data, err := proto.Marshal(app)
			if err != nil {
				logger.Println("Marshaling error: ", err)
				// stop processing
				errors_count++
			}
			mc.Set(&memcache.Item{Key: key, Value: data})
		} else {
			logger.Println("We have error during processing file:", filename)
		}

	}

	logger.Printf("File %s already processed. Errors: %.2f%%\n", filename, 100*float32(errors_count)/float32(apps_count))

}

func dot_rename(fpath string) {
	dir, file := filepath.Split(fpath)
	file = "." + file
	os.Rename(fpath, filepath.Join(dir, file))
}

func worker(filename_channel chan string, wg *sync.WaitGroup, memc_clients map[string]*memcache.Client) {
	defer wg.Done()
	for filename := range filename_channel {
		process_file(filename, memc_clients)
		dot_rename(filename)
	}
}

func main() {
	var wg sync.WaitGroup

	flag.StringVar(&default_opt.idfa, "idfa", default_opt.idfa, "idfa memcache address")
	flag.StringVar(&default_opt.gaid, "gaid", default_opt.gaid, "gaid memcache address")
	flag.StringVar(&default_opt.adid, "adid", default_opt.adid, "adid memcache address")
	flag.StringVar(&default_opt.dvid, "dvid", default_opt.dvid, "dvid memcache address")
	flag.StringVar(&default_opt.pattern, "pattern", default_opt.pattern, "pattern")
	flag.StringVar(&default_opt.logfile, "logfile", default_opt.logfile, "logfile name")
	flag.IntVar(&default_opt.threads, "threads", default_opt.threads, "number of threads")
	flag.Parse()

	if default_opt.logfile != "" {
		logoutput, err := os.OpenFile(default_opt.logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			fmt.Println("Error opening logfile", err)
		} else {
			logger.SetOutput(logoutput)
			defer logoutput.Close()
		}

	}

	device_memc := map[string]string{
		"idfa": default_opt.idfa,
		"gaid": default_opt.gaid,
		"adid": default_opt.adid,
		"dvid": default_opt.dvid,
	}

	memc_clients := make(map[string]*memcache.Client)
	fchan := make(chan string)

	logger.Println("Creating memcache clients.")
	for key, value := range device_memc {
		memc_clients[key] = memcache.New(value)
	}

	logger.Println("Creating threads for file processing")
	for i := 0; i < default_opt.threads; i++ {
		wg.Add(1)
		go worker(fchan, &wg, memc_clients)
	}

	logger.Println("Searching files by pattern. Except hidden files.")
	files, _ := filepath.Glob(default_opt.pattern)
	for _, filename := range files {
		if _, file := filepath.Split(filename); !strings.HasPrefix(file, ".") {
			fchan <- filename
		}
	}

	close(fchan)

	wg.Wait()

	logger.Println("Done!")
}
